DUCKLAKE — A LOCAL ANALYTICAL DATA LAKE PIPELINE

Overview

DuckLake is a Python-based local analytical data lake pipeline for NYC Yellow Taxi data. It connects to a Supabase (PostgreSQL) metadata database in read-only mode, extracts trip records in Parquet format, and loads them into an in-memory DuckDB instance for fast SQL analytics. The pipeline performs basic transformations (such as creating reference tables and merging monthly datasets) and supports time-travel queries via snapshot metadata.
Features

    In-Memory Analytics with DuckDB
    Leverages DuckDB to query data directly in-memory for high performance.

    Modular ETL Pipeline
    Cleanly separates extraction, loading, and transformation steps for maintainability.

    Read-Only Supabase Connection
    Attaches a Supabase (Postgres) database as a read-only source for table metadata and snapshots.

    Zone & Vendor Lookups
    Includes preloaded lookup tables for taxi zones and vendor names to enrich trip data.

    Time-Travel Queries
    Supports querying historical data snapshots (time-travel) through the metadata layer.

Installation (using uv)

    Set up virtual environment

uv venv

Activate it:

    Linux / macOS:

source .venv/bin/activate

Windows:

    .\.venv\Scripts\activate

Install dependencies

    uv pip install -r requirements.txt

    This installs DuckDB, pandas, requests, python-dotenv, and other required packages.

Usage

Run the ETL pipeline to load and transform data:

python taxi_data.py

    Connects to DuckDB and Supabase

    Creates lookup tables (vendor_names, taxi_zone_lookup)

    Imports all available NYC Yellow Taxi trip records into a local DuckDB table (taxi_data)
