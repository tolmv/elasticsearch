import psycopg2
from psycopg2.extras import execute_values
import logging
from config import settings

logger = logging.getLogger(__name__)

class PostgresClient:
    def __init__(self, database_url):
        self.conn = psycopg2.connect(database_url, application_name='product_matching_service')
        self.conn.autocommit = False

    def insert_products(self, products):
        try:
            with self.conn.cursor() as cur:
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
            self.conn.commit()
            logger.info(f"Inserted {len(products)} products into PostgreSQL")
        except psycopg2.Error as e:
            logger.error(f"Error inserting products into PostgreSQL: {e}")
            self.conn.rollback()

    def fetch_products(self, batch_size=settings.CHUNK_SIZE):
        with self.conn.cursor(name='fetch_products_cursor') as cursor:
            cursor.execute("SELECT uuid, title, description, brand FROM public.sku")
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                columns = [desc[0] for desc in cursor.description]
                batch = [dict(zip(columns, row)) for row in rows]
                logger.debug(f"Fetched batch size: {len(batch)}")
                yield batch

    def update_similar_products(self, product_uuid, similar_uuids):
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    "UPDATE public.sku SET similar_sku = %s WHERE uuid = %s",
                    (similar_uuids, product_uuid)
                )
            self.conn.commit()
            logger.info(f"Updated similar products for {product_uuid}")
        except psycopg2.Error as e:
            logger.error(f"Error updating similar products: {e}")
            self.conn.rollback()

    def close(self):
        if self.conn:
            self.conn.close()
            logger.info("Closed PostgreSQL connection")