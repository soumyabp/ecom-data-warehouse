"""
Loads raw CSV files from the data/ directory into a local DuckDB database.
This is the Extract + Load step of the ELT pipeline. Once loaded, dbt handles
the Transform step (staging -> marts).
"""

import os
import duckdb

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(BASE_DIR, "dbt_project", "warehouse.duckdb")

RAW_TABLES = {
    "raw_orders": "olist_orders_dataset.csv",
    "raw_customers": "olist_customers_dataset.csv",
    "raw_order_items": "olist_order_items_dataset.csv",
    "raw_order_payments": "olist_order_payments_dataset.csv",
    "raw_products": "olist_products_dataset.csv",
    "raw_sellers": "olist_sellers_dataset.csv",
}


def load_csv_to_duckdb(con, table_name, csv_filename):
    csv_path = os.path.join(DATA_DIR, csv_filename)
    if not os.path.exists(csv_path):
        print(f"  SKIP: {csv_path} not found")
        return

    con.execute(f"DROP TABLE IF EXISTS raw.{table_name}")
    con.execute(
        f"CREATE TABLE raw.{table_name} AS SELECT * FROM read_csv_auto('{csv_path}')"
    )
    row_count = con.execute(f"SELECT count(*) FROM raw.{table_name}").fetchone()[0]
    print(f"  Loaded {table_name}: {row_count:,} rows")


def main():
    print(f"Database: {DB_PATH}")
    print(f"Data directory: {DATA_DIR}\n")

    con = duckdb.connect(DB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")

    for table_name, csv_file in RAW_TABLES.items():
        load_csv_to_duckdb(con, table_name, csv_file)

    con.close()
    print("\nDone. Raw tables loaded into DuckDB.")


if __name__ == "__main__":
    main()
