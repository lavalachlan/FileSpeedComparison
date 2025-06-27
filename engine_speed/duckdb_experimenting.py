from warnings import filters
import pandas as pd
import polars as pl
import duckdb
from datetime import datetime
import glob

# Benchmark .py file
import benchmark as bm


# parameters
SINGLE_FILE = 'data/taxi_2019_04.parquet'
MULTI_FILE_PATH = "data/taxi/*01.parquet"
MULTI_FILES = glob.glob(MULTI_FILE_PATH)

FILTER = [("pickup_at", ">", datetime(2019, 6, 30))]
COLUMNS = ["pickup_at"]
SHOW_PLOTS = False
REPEAT_TIMES = 1
READ_SINGLE_FILE = False
if READ_SINGLE_FILE:
    TITLE_START = "Single File Benchmarks"
else:
    TITLE_START = "Multiple File Benchmarks"

# init
con = duckdb.connect()

def duckdb_read_all():
    return con.execute(f"SELECT * FROM '{SINGLE_FILE}'")
def duckdb_filter_all():
    return con.execute(f"""
        SELECT *
        FROM '{SINGLE_FILE}'
        WHERE pickup_at > '2019-06-30'
    """)
def duckdb_filter_one():
    return con.execute(f"""
        SELECT pickup_at
        FROM '{SINGLE_FILE}'
        WHERE pickup_at > '2019-06-30'
    """)
def duckdb_filter_count():
    return con.execute(f"""
        SELECT count(*)
        FROM '{SINGLE_FILE}'
        WHERE pickup_at > '2019-06-30'
    """)
def duckdb_read_multi():
    return con.execute(f"SELECT * FROM '{MULTI_FILE_PATH}'")

def duckdb_read_all_df():
    return con.execute(f"SELECT * FROM '{SINGLE_FILE}'").fetchdf()
def duckdb_filter_all_df():
    return con.execute(f"""
        SELECT *
        FROM '{SINGLE_FILE}'
        WHERE pickup_at > '2019-06-30'
    """).fetchdf()
def duckdb_filter_one_df():
    return con.execute(f"""
        SELECT pickup_at
        FROM '{SINGLE_FILE}'
        WHERE pickup_at > '2019-06-30'
    """).fetchdf()
def duckdb_filter_count_df():
    return con.execute(f"""
        SELECT count(*)
        FROM '{SINGLE_FILE}'
        WHERE pickup_at > '2019-06-30'
    """).fetchdf()
def duckdb_read_multi_df():
    return con.execute(f"SELECT * FROM '{MULTI_FILE_PATH}'").fetchdf()

results_all = {
    "DuckDB Read All": bm.timing(duckdb_read_all, runs=REPEAT_TIMES, label="DuckDB Read All"),
    "DuckDB Filter All": bm.timing(duckdb_filter_all, runs=REPEAT_TIMES, label="DuckDB Filter All"),
    "DuckDB Filter One": bm.timing(duckdb_filter_one, runs=REPEAT_TIMES, label="DuckDB Filter One"),
    "DuckDB Filter Count": bm.timing(duckdb_filter_count, runs=REPEAT_TIMES, label="DuckDB Filter Count"),
    "DuckDB Read Multi": bm.timing(duckdb_read_multi, runs=REPEAT_TIMES, label="DuckDB Read Multi"),
    "DuckDB Read All DF": bm.timing(duckdb_read_all_df, runs=REPEAT_TIMES, label="DuckDB Read All DF"),
    "DuckDB Filter All DF": bm.timing(duckdb_filter_all_df, runs=REPEAT_TIMES, label="DuckDB Filter All DF"),
    "DuckDB Filter One DF": bm.timing(duckdb_filter_one_df, runs=REPEAT_TIMES, label="DuckDB Filter One DF"),
    "DuckDB Filter Count DF": bm.timing(duckdb_filter_count_df, runs=REPEAT_TIMES, label="DuckDB Filter Count DF"),
    "DuckDB Read Multi DF": bm.timing(duckdb_read_multi_df, runs=REPEAT_TIMES, label="DuckDB Read Multi DF")
}

bm.plot_results(results_all, f"{TITLE_START}: Read All Data", "speed_duckdb_and_df.png")
