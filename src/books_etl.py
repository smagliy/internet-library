import psycopg2
import os
import logging

from contextlib import contextmanager
from typing import List
from datetime import datetime


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


@contextmanager
def get_db_connection():
    conn = psycopg2.connect(
        host=os.environ['DB_HOST'],
        port=os.environ['DB_PORT'],
        dbname=os.environ['DB_NAME'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD']
    )
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        logging.error("Database error occurred, transaction rolled back.")
        raise e
    finally:
        conn.close()


class BookETL:
    def __init__(self, conn, cut_off_date: str):
        self.conn = conn
        self.cut_off_date = cut_off_date

    def extract(self, fields: List[str]):
        try:
            with self.conn.cursor() as cur:
                columns = ', '.join(fields)
                query = f"""
                    SELECT {columns}
                    FROM books
                    WHERE last_updated >= %s
                """
                cur.execute(query, (self.cut_off_date,))
                rows = cur.fetchall()
                logging.info(f"[Extract] Retrieved {len(rows)} records.")
                return rows
        except Exception as e:
            logging.exception("[Extract] Failed to extract data.")
            raise

    @staticmethod
    def transform(raw_data):
        try:
            transformed = []
            for row in raw_data:
                book_id, title, price, genre, stock_quantity, last_updated = row
                original_price = price
                rounded_price = round(price, 1)
                price_category = 'budget' if rounded_price < 500 else 'premium'
                transformed.append((
                    book_id, title, original_price, rounded_price, genre, price_category
                ))
            logging.info(f"Transformed {len(transformed)} records.")
            return transformed
        except Exception as e:
            logging.exception(f"Failed to transform data: {e}")
            raise

    def load(self, transformed_data):
        try:
            with self.conn.cursor() as cur:
                cur.executemany("""
                    INSERT INTO books_processed (
                        book_id, title, original_price, rounded_price, genre, price_category
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, transformed_data)
            logging.info(f"Inserted {len(transformed_data)} records into books_processed")
        except Exception as e:
            logging.exception(f"Failed to load data into books_processed: {e}")
            raise

    def main(self):
        try:
            datetime.strptime(self.cut_off_date, "%Y-%m-%d")
            raw = self.extract(['book_id', 'title', 'price', 'genre', 'stock_quantity', 'last_updated'])
            transformed = self.transform(raw)
            self.load(transformed)
            logging.info("ETL process completed successfully.")
        except ValueError:
            logging.error("Invalid date format. Please use YYYY-MM-DD.")
        except Exception as e:
            logging.exception(f"ETL process failed: {e}")


if __name__ == '__main__':
    import sys
    if len(sys.argv) != 2:
        logging.error("Usage: python books_etl.py YYYY-MM-DD")
        sys.exit(1)
    cut_off = sys.argv[1]
    with get_db_connection() as conn:
        etl = BookETL(conn, cut_off)
        etl.main()
