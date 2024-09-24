import lxml.etree as ET
import uuid
import logging
from typing import List, Dict, Any, Generator
import psycopg2
from psycopg2.extras import execute_values
from elasticsearch import Elasticsearch, helpers
import os
from concurrent.futures import ThreadPoolExecutor
import time
import sys

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


FETCH_PRODUCTS_SQL = "SELECT uuid, title, description, brand FROM public.sku"
UPDATE_PRODUCT_SQL = "UPDATE public.sku SET similar_sku = ARRAY[%s]::uuid[] WHERE uuid = %s;"


def parse_categories(context: ET.iterparse) -> Dict[int, List[str]]:
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


def parse_products(context: ET.iterparse, categories: Dict[int, List[str]]) -> Generator[Dict[str, Any], None, None]:
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

def insert_products(conn, products: List[Dict[str, Any]]):
    try:
        with conn.cursor() as cur:
            query = """
            INSERT INTO public.sku (
                uuid, marketplace_id, product_id, title, description, brand, 
                seller_id, seller_name, first_image_url, category_id, 
                category_lvl_1, category_lvl_2, category_lvl_3, category_remaining,
                price_before_discounts, discount, price_after_discounts, currency, barcode
            ) VALUES %s
            """
            values = [
                (
                    p["uuid"], p["marketplace_id"], p["product_id"], p["title"], 
                    p["description"], p["brand"], p["seller_id"], p["seller_name"], 
                    p["first_image_url"], p["category_id"], p["category_lvl_1"],
                    p["category_lvl_2"], p["category_lvl_3"], p["category_remaining"],
                    p["price_before_discounts"], p["discount"], p["price_after_discounts"],
                    p["currency"], p["barcode"]
                ) for p in products
            ]
            execute_values(cur, query, values)
        conn.commit()
        logger.info(f"Inserted {len(products)} products into PostgreSQL")
    except psycopg2.Error as e:
        logger.error(f"Error inserting products into PostgreSQL: {e}")
        conn.rollback()


def index_products(es: Elasticsearch, products: List[Dict[str, Any]]):
    try:
        actions = [
            {
                "_index": "products",
                "_id": product["uuid"],
                "_source": product
            }
            for product in products
        ]
        logger.info(f"Prepared actions: {actions}")
        success, failed = helpers.bulk(es, actions)
        logger.info(f"Indexed {success} products in Elasticsearch.")
        if failed:
            logger.error(f"Failed operations: {failed}")
    except Exception as e:
        logger.error(f"Error indexing products in Elasticsearch: {e}")


def find_similar_products(es: Elasticsearch, product: Dict[str, Any]) -> List[str]:
    try:
        if not isinstance(product, dict) or "uuid" not in product:
            logger.error(f"Invalid product format: {product}")
            return []

        query = {
            "query": {
                "more_like_this": {
                    "fields": ["title", "description", "brand"],
                    "like": [{
                        "_index": "products",
                        "_id": product["uuid"]
                    }],
                    "min_term_freq": 1,
                    "max_query_terms": 12
                }
            }
        }
        
        logger.info(f"Executing ES query for product {product['uuid']}: {query}")
        result = es.search(index="products", body=query, size=5)
        logger.info(f"ES result for product {product['uuid']}: {result}")

        if "hits" in result and "hits" in result["hits"]:
            similar_skus = [hit["_id"] for hit in result["hits"]["hits"] if hit["_id"] != product["uuid"]]
            logger.info(f"Similar SKUs for product {product['uuid']}: {similar_skus}")
            return similar_skus
        else:
            logger.error(f"Unexpected ES result format for product {product['uuid']}: {result}")
            return []

    except Exception as e:
        logger.error(f"Error finding similar products for product {product['uuid']}:\n{e}")
        return []


def update_similar_products(conn, product_uuid: str, similar_uuids: List[str]):
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE public.sku SET similar_sku = %s WHERE uuid = %s",
                (similar_uuids, product_uuid)
            )
        conn.commit()
        logger.info(f"1Updateds similar products for {product_uuid}")
    except psycopg2.Error as e:
        logger.error(f"Error updating similar products: {e}")
        conn.rollback()


def process_chunk(chunk: List[Dict[str, Any]], conn, es: Elasticsearch):
    insert_products(conn, chunk)
    index_products(es, chunk)



def connect_with_retry(connect_func, max_retries=5, delay=5):
    retries = 0
    while retries < max_retries:
        try:
            return connect_func()
        except Exception as e:
            retries += 1
            logger.warning(f"Connection attempt {retries} failed: {e}")
            if retries == max_retries:
                logger.error(f"Failed to connect after {max_retries} attempts")
                raise
            time.sleep(delay)


def create_products_index(es: Elasticsearch):
    index_body = {
        "mappings": {
            "properties": {
                "uuid": {"type": "keyword"},
                "marketplace_id": {"type": "integer"},
                "product_id": {"type": "long"},
                "title": {"type": "text"},
                "description": {"type": "text"},
                "brand": {"type": "keyword"},
                "seller_id": {"type": "integer"},
                "seller_name": {"type": "keyword"},
                "first_image_url": {"type": "text"},
                "category_id": {"type": "integer"},
                "category_lvl_1": {"type": "keyword"},
                "category_lvl_2": {"type": "keyword"},
                "category_lvl_3": {"type": "keyword"},
                "category_remaining": {"type": "text"},
                "price_before_discounts": {"type": "float"},
                "discount": {"type": "float"},
                "price_after_discounts": {"type": "float"},
                "currency": {"type": "keyword"},
                "barcode": {"type": "long"}
            }
        }
    }

    if not es.indices.exists(index="products"):
        try:
            es.indices.create(index="products", body=index_body)
            logger.info("Index 'products' created successfully")
        except Exception as e:
            logger.error(f"Failed to create index 'products': {e}")
    else:
        logger.info("Index 'products' already exists")


def find_and_update_similar_products(es: Elasticsearch, conn, product: Dict[str, Any]):
    try:
        similar_skus = find_similar_products(es, product)
        
        with conn.cursor() as cursor:
            update_similar_products_in_db(cursor, product["uuid"], similar_skus)
            conn.commit()  
        logger.info(f"Updated product {product['uuid']} with similar SKUs: {similar_skus}")
        
    except Exception as e:
        logger.error(f"Error processing product {product['uuid']}: {e}")
        if conn:
            conn.rollback()  
        return None  
    return product['uuid']


def fetch_products_from_db(conn, batch_size=1000):
    with conn.cursor(name='fetch_products_cursor') as cursor:  
        cursor.execute(FETCH_PRODUCTS_SQL)
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            columns = [desc[0] for desc in cursor.description]
            batch = [dict(zip(columns, row)) for row in rows]
            logger.debug(f"Fetched batch size: {len(batch)}")
            yield batch


def update_similar_products_in_db(cursor, uuid, similar_skus: List[str]):
    cursor.execute(UPDATE_PRODUCT_SQL, (similar_skus, uuid))


def main():
    logger.info("Starting product matching service")

    conn = None
    es = None

    try:
        conn = connect_with_retry(lambda: psycopg2.connect(os.environ["DATABASE_URL"], application_name='product_matching_service'))
        logger.info("Connected to PostgreSQL")
        conn.autocommit = False  

        es = connect_with_retry(lambda: Elasticsearch([os.environ["ELASTICSEARCH_URL"]]))
        logger.info("Connected to Elasticsearch")

        file_path = "elektronika_products_20240924_074730.xml"
        context = ET.iterparse(file_path, events=("start", "end"))

        categories = parse_categories(context)
        logger.info(f"Parsed {len(categories)} categories")

        context = ET.iterparse(file_path, events=("start", "end"))

        chunk_size = 1000
        chunk = []
        start_time = time.time()

        with ThreadPoolExecutor(max_workers=5) as executor:
            for product in parse_products(context, categories):
                chunk.append(product)
                if len(chunk) >= chunk_size:
                    executor.submit(process_chunk, chunk, conn, es)
                    chunk = []

            if chunk:  
                executor.submit(process_chunk, chunk, conn, es)

        end_time = time.time()
        logger.info(f"Processed XML and added to PostgreSQL in {end_time - start_time:.2f} seconds")

        create_products_index(es)
        
        product_count = 0
        all_products = []  

        for i, batch in enumerate(fetch_products_from_db(conn, batch_size=1000)):
            logger.info(f"Fetched batch {i+1} with {len(batch)} products")  
            index_products(es, batch)  
            all_products.extend(batch)  
            for product in batch:
                logger.debug(f"Product in batch: {product}")

        logger.info(f"Total products fetched: {len(all_products)}")

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(find_and_update_similar_products, es, conn, product)
                for product in all_products
            ]
            for future in futures:
                result = future.result()  
                if result:
                    logger.debug(f"Task completed with result: {result}")
                    product_count += 1

        end_time = time.time()
        logger.info(f"Processed {product_count} products in {end_time - start_time:.2f} seconds")


    except Exception as e:
        logger.error(f"An error occurred: {e}")
        sys.exit(1)
    finally:
        if conn:
            conn.close()
            logger.info("Closed PostgreSQL connection")
        if es:
            es.transport.close()
            logger.info("Closed Elasticsearch connection")


if __name__ == "__main__":
    main()
