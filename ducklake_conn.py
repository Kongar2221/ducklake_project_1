import os
import duckdb
from dotenv import load_dotenv

load_dotenv()

def local_ducklake_conn():
    print("Setting up database connection...")
    USER     = os.getenv("user")
    PASSWORD = os.getenv("password")
    HOST     = os.getenv("host")
    PORT     = os.getenv("port")
    DBNAME   = os.getenv("dbname")
    bucket_path = 'C:/work/projects/taxi_bucket'  # or wherever your local data lives

    # Init DuckDB in memory
    conn = duckdb.connect(database=":memory:")
    conn.execute("INSTALL httpfs; INSTALL postgres; INSTALL ducklake;")
    conn.execute("LOAD httpfs; LOAD postgres; LOAD ducklake;")

    # Attach your Postgres-backed DuckLake catalog
    attach = (
        f"ATTACH 'ducklake:postgres:"
        f"dbname={DBNAME} host={HOST} port={PORT} user={USER} password={PASSWORD}' "
        f"AS my_ducklake (DATA_PATH '{bucket_path}');"
    )
    print("Attaching DuckLake database...")
    conn.execute(attach)
    conn.execute("USE my_ducklake;")
    print("DuckLake database attached and selected.")

    return conn

if __name__ == "__main__":
    # Quick smoke-test
    conn = local_ducklake_conn()
    print(conn.execute("SHOW TABLES;").fetchall())
