import os
import requests
import time
from dotenv import load_dotenv
from ducklake_conn import local_ducklake_conn

load_dotenv()

def create_vendor_table(conn):
    exists = conn.execute(
        "select count(*) from information_schema.tables where table_name='vendor_names';"
    ).fetchone()[0]
    if exists:
        return
    conn.execute(
        "create table vendor_names (vendorid integer, vendor_name varchar);"
    )
    conn.execute(
        "insert into vendor_names (vendorid, vendor_name) "
        "values (1, 'Creative Mobile Technologies'), (2, 'VeriFone Inc.');"
    )

def create_rate_code_table(conn):
    exists = conn.execute(
        "select count(*) from information_schema.tables where table_name='rate_code_names';"
    ).fetchone()[0]
    if exists:
        return
    conn.execute(
        "create table rate_code_names (ratecodeid integer, rate_code_description varchar);"
    )
    conn.execute(
        "insert into rate_code_names (ratecodeid, rate_code_description) values "
        "(1, 'Standard rate'), (2, 'JFK'), (3, 'Newark'), (4, 'Nassau or Westchester'), "
        "(5, 'Negotiated fare'), (6, 'Group ride');"
    )

def create_zone_table(conn):
    exists = conn.execute(
        "select count(*) from information_schema.tables where table_name='taxi_zone_lookup';"
    ).fetchone()[0]
    if exists:
        return
    conn.execute(
        "create table taxi_zone_lookup (locationid integer, borough varchar, zone varchar, service_zone varchar);"
    )
    conn.execute(
        "insert into taxi_zone_lookup "
        "select * from read_csv_auto('taxi_zone_lookup.csv', header=true);"
    )

def register_parquet_files(conn):
    first = False
    for year in range(2022, 2025):
        for month in range(1, 13):
            if year == 2024 and month > 12:
                break
            url = (
                f"https://d37ci6vzurychx.cloudfront.net/"
                f"trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
            )
            try:
                r = requests.head(url, timeout=5)
                if r.status_code != 200:
                    continue
                if not first:
                    conn.execute(
                        f"create table if not exists taxi_data as "
                        f"select * from read_parquet('{url}') where 1=0;"
                    )
                    first = True
                conn.execute(
                    f"insert into taxi_data select * from read_parquet('{url}');"
                )
                time.sleep(5)
            except Exception:
                continue
    if not first:
        print("no valid files found. exiting.")

def main():
    conn = local_ducklake_conn()
    create_vendor_table(conn)
    create_rate_code_table(conn)
    create_zone_table(conn)
    register_parquet_files(conn)
    conn.close()

if __name__ == "__main__":
    main()