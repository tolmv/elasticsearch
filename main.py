import logging
import time
import sys
from concurrent.futures import ThreadPoolExecutor
import lxml.etree as ET
import uuid
from elasticsearch_client import ElasticsearchClient
from postgres_client import PostgresClient
from config import settings

logging.basicConfig(level=settings.LOG_LEVEL, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def parse_categories(context: ET.iterparse):
    categories = {}
    for event, elem in context:
        if event == "end" and elem.tag == "category":
            try:
                category_id = int(elem.get("id"))
                parent_id = int(elem.get("parentId", 0))
                name = elem.text
                if parent_id == 0:
                    categories[category_id] = [name]
                else:
                    categories[category_id] = categories.get(parent_id, []) + [name]
            except ValueError as e:
                logger.error(f"Error parsing category: {e}")
            finally:
                elem.clear()
    return categories


def parse_products(context: ET.iterparse, categories):
    for event, elem in context:
        if event == "end" and elem.tag == "offer":
            try:
                category_id = int(elem.findtext("categoryId", 0))
                category_path = categories.get(category_id, [])
                product = {
                    "uuid": str(uuid.uuid4()),
                    "marketplace_id": 1,
                    "product_id": int(elem.get("id")),
                    "title": elem.findtext("name"),
                    "description": elem.findtext("description"),
                    "brand": elem.findtext("vendor"),
                    "seller_id": int(elem.findtext("shop-id", 0)),
                    "seller_name": elem.findtext("shop-name"),
                    "first_image_url": elem.findtext("picture"),
                    "category_id": category_id,
                    "category_lvl_1": category_path[0] if len(category_path) > 0 else None,
                    "category_lvl_2": category_path[1] if len(category_path) > 1 else None,
                    "category_lvl_3": category_path[2] if len(category_path) > 2 else None,
                    "category_remaining": "/".join(category_path[3:]) if len(category_path) > 3 else None,
                    "price_before_discounts": float(elem.findtext("price", 0)),
                    "discount": float(elem.findtext("discount", 0)),
                    "price_after_discounts": float(elem.findtext("oldprice", 0)) or float(elem.findtext("price", 0)),
                    "currency": elem.findtext("currencyId"),
                    "barcode": int(elem.findtext("barcode", 0)),
                }
                yield product
            except (ValueError, AttributeError) as e:
                logger.error(f"Error parsing product: {e}")
            finally:
                elem.clear()
def process_chunk(chunk, pg_client: PostgresClient, es_client: ElasticsearchClient):
    pg_client.insert_products(chunk)
    es_client.index_products(chunk)

def find_and_update_similar_products(es_client: ElasticsearchClient, pg_client: PostgresClient, product):
    try:
        similar_skus = es_client.find_similar_products(product)
        pg_client.update_similar_products(product["uuid"], similar_skus)
        logger.info(f"Updated product {product['uuid']} with similar SKUs: {similar_skus}")
    except Exception as e:
        logger.error(f"Error processing product {product['uuid']}: {e}")
        return None
    return product['uuid']


def main():
    logger.info("Starting product matching service")

    try:
        pg_client = PostgresClient(settings.DATABASE_URL)
        es_client = ElasticsearchClient(settings.ELASTICSEARCH_URL)

        context = ET.iterparse(settings.XML_FILE_PATH, events=("start", "end"))

        categories = parse_categories(context)
        logger.info(f"Parsed {len(categories)} categories")

        context = ET.iterparse(settings.XML_FILE_PATH, events=("start", "end"))

        chunk = []
        start_time = time.time()

        with ThreadPoolExecutor(max_workers=settings.MAX_WORKERS) as executor:
            for product in parse_products(context, categories):
                chunk.append(product)
                if len(chunk) >= settings.CHUNK_SIZE:
                    executor.submit(process_chunk, chunk, pg_client, es_client)
                    chunk = []

            if chunk:
                executor.submit(process_chunk, chunk, pg_client, es_client)

        end_time = time.time()
        logger.info(f"Processed XML and added to PostgreSQL in {end_time - start_time:.2f} seconds")

        es_client.create_products_index()
        
        product_count = 0
        all_products = []

        for i, batch in enumerate(pg_client.fetch_products(batch_size=settings.CHUNK_SIZE)):
            logger.info(f"Fetched batch {i+1} with {len(batch)} products")
            es_client.index_products(batch)
            all_products.extend(batch)

        logger.info(f"Total products fetched: {len(all_products)}")

        with ThreadPoolExecutor(max_workers=settings.MAX_WORKERS) as executor:
            futures = [
                executor.submit(find_and_update_similar_products, es_client, pg_client, product)
                for product in all_products
            ]
            for future in futures:
                result = future.result()
                if result:
                    product_count += 1

        end_time = time.time()
        logger.info(f"Processed {product_count} products in {end_time - start_time:.2f} seconds")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        sys.exit(1)
    finally:
        if 'pg_client' in locals():
            pg_client.close()
        if 'es_client' in locals():
            es_client.close()

if __name__ == "__main__":
    main()