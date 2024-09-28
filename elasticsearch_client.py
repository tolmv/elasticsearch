from elasticsearch import Elasticsearch, helpers
import logging
from config import settings

logger = logging.getLogger(__name__)

class ElasticsearchClient:
    def __init__(self, elasticsearch_url):
        self.es = Elasticsearch([elasticsearch_url])

    def create_products_index(self):
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

        if not self.es.indices.exists(index="products"):
            try:
                self.es.indices.create(index="products", body=index_body)
                logger.info("Index 'products' created successfully")
            except Exception as e:
                logger.error(f"Failed to create index 'products': {e}")
        else:
            logger.info("Index 'products' already exists")

    def index_products(self, products):
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
            success, failed = helpers.bulk(self.es, actions)
            logger.info(f"Indexed {success} products in Elasticsearch.")
            if failed:
                logger.error(f"Failed operations: {failed}")
        except Exception as e:
            logger.error(f"Error indexing products in Elasticsearch: {e}")

    def find_similar_products(self, product):
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
            result = self.es.search(index="products", body=query, size=5)
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

    def close(self):
        self.es.transport.close()
        logger.info("Closed Elasticsearch connection")