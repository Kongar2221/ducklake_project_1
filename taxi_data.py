#!/usr/bin/env python3
import os
import duckdb
from dotenv import load_dotenv
import ducklake_conn

def create_vendor_table(conn):
    # Only create and fill the table if it does not exist
    if table_exists(conn, 'vendor_names'):
        return
    conn.execute("""
        CREATE TABLE vendor_names (
            vendorid INTEGER,
            vendor_name VARCHAR
        )
    """)
    # TODO: Populate vendor_names from a lookup source


def create_rate_code_table(conn):
    # Only create and fill the table if it does not exist
    if table_exists(conn, 'rate_code_names'):
        return
    conn.execute("""
        CREATE TABLE rate_code_names (
            ratecodeid INTEGER,
            rate_code_description VARCHAR
        )
    """)
    # TODO: Populate rate_code_names from a lookup source


def table_exists(conn, table_name):
    result = conn.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'").fetchone()
    return result[0] > 0


def register_parquet_files(conn, data_dir: str):
    """
    Discover and register Parquet files in the given directory into DuckDB.
    """
    files = os.listdir(data_dir)
    parquet_urls = [os.path.join(data_dir, f) for f in files if f.endswith('.parquet')]

    first_file_found = False
    for url in parquet_urls:
        try:
            if not first_file_found:
                conn.execute(f"CREATE TABLE taxi_data AS SELECT * FROM read_parquet('{url}')")
                first_file_found = True
            else:
                conn.execute(f"INSERT INTO taxi_data SELECT * FROM read_parquet('{url}')")
        except Exception as e:
            print(f"Error checking {url}: {e}")
    if not first_file_found:
        print("No valid files found. Exiting.")
        return


def main():
    load_dotenv()
    # Establish connection via ducklake_conn helper
    conn = ducklake_conn.local_ducklake_conn()

    # Paths
    DATA_DIR = os.getenv('DATA_PATH', './data')

    # Create lookup tables
    create_vendor_table(conn)
    create_rate_code_table(conn)

    # Register taxi parquet files into DuckDB
    register_parquet_files(conn, DATA_DIR)

    # Example analytics: count trips per vendor
    result = conn.execute("SELECT vendorid, COUNT(*) AS trip_count FROM taxi_data GROUP BY vendorid").fetchdf()
    print(result)

    # Close the connection
    conn.close()


if __name__ == "__main__":
    main()
