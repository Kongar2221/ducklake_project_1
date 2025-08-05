import os
import duckdb
from dotenv import load_dotenv

load_dotenv()

def snapshot_conn():
    user = os.getenv("snap_user")
    pw = os.getenv("snap_password")
    host = os.getenv("snap_host")
    port = os.getenv("snap_port", "5432")
    dbname = os.getenv("snap_dbname")

    conn = duckdb.connect(database=":memory:", read_only=False)
    conn.execute("install postgres;")
    conn.execute("load postgres;")
    conn.execute(
        f"attach 'dbname={dbname} host={host} port={port} user={user} password={pw}' as snapshot(type postgres);"
    )
    return conn

def local_ducklake_conn():
    data_path = os.getenv("data_path", "./data")
    duckdb_path = os.getenv("duckdb_path", ":memory:")

    conn = duckdb.connect(database=":memory:", read_only=False)
    conn.execute("install ducklake;")
    conn.execute("load ducklake;")
    conn.execute(
        f"attach 'ducklake:{duckdb_path}' as dl(data_path '{data_path}');"
    )
    conn.execute("use dl;")
    return conn

if __name__ == "__main__":
    snap = snapshot_conn()
    snap.execute("use snapshot;")
    print(snap.execute("show tables;").fetchall())
    snap.close()

    dl = local_ducklake_conn()
    print(dl.execute("show tables;").fetchall())
    dl.close()