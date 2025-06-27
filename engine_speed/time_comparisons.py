from warnings import filters
import pandas as pd
import polars as pl
import duckdb
from datetime import datetime
import glob

# Benchmark .py file
import benchmark as bm

'''
Comparing speeds of Pandas, Polars, and DuckDB against various functions on Parquet files such as reading and filtering.
This script benchmarks various read and filter operations on chosen Parquet files using Pandas, Polars, and DuckDB.
It includes functions to read all data, filter data by a date condition, and count rows, as well as miscellaneous
comparisons between Polars LazyFrame and DataFrame, and different Pandas read methods.

Pandas uses read_parqet which loads the data into memory. Polars has the option to use scan_parquet then collect which is
significantly more memory efficient. DuckDB is very fast since it uses SQL, however, this benchmark does not return a df
after a DuckDB query. Take that as you will. It's more focused on the query itself.

This script uses the `benchmark` module to measure the time taken for each operation and plot the results.


Variables:
- `SINGLE_FILE`: A single Parquet file to be used for benchmarking.
- `MULTI_FILES`: Multiple Parquet files to be used for benchmarking.
- `FILTER`: The row filter to be applied.
- `COLUMNS`: The column filter to be applied.
- `TITLE_START`: The start of the title to use for each output.
- `REPEAT_TIMES`: Number of times each function will be run to average the time taken.
- `con`: DuckDB connection object for executing SQL queries.
'''

# parameters
SINGLE_FILE = 'data/taxi_2019_04.parquet'
MULTI_FILE_PATH = "data/taxi/yellow*.parquet"
MULTI_FILES = glob.glob(MULTI_FILE_PATH)

FILTER = [("pickup_at", ">", datetime(2019, 6, 30))]
COLUMNS = ["pickup_at"]
SHOW_PLOTS = False
REPEAT_TIMES = 5
READ_SINGLE_FILE = True
if READ_SINGLE_FILE:
    TITLE_START = "Single File Benchmarks"
else:
    TITLE_START = "Multiple File Benchmarks"

# init
con = duckdb.connect()

if READ_SINGLE_FILE:
    ### Functions to compare on speeds - for one parquet SINGLE_FILE 'alltaxi.parquet'
    # Ordinary read functions on all data without filters
    def pandas_read_all():
        return pd.read_parquet(SINGLE_FILE)
    def polars_read_all():
        return pl.scan_parquet(SINGLE_FILE).collect()
    def duckdb_read_all():
        return con.execute(f"SELECT * FROM '{SINGLE_FILE}'")

    # Filtering function 1 - read ALL columns with filter on pickup_at > '2019-06-30'
    def pandas_filter_all():
        return pd.read_parquet(SINGLE_FILE, filters=[("pickup_at", ">", datetime(2019, 6, 30))])
    def polars_filter_all():
        return (
            pl.scan_parquet(SINGLE_FILE)
            .filter(pl.col("pickup_at").dt.date() > pl.datetime(2019, 6, 30))
            .collect()
        )
    def duckdb_filter_all():
        return con.execute(f"""
            SELECT *
            FROM '{SINGLE_FILE}'
            WHERE pickup_at > '2019-06-30'
        """)

    # Filtering function 2 - read ONE column with filter on pickup_at > '2019-06-30'
    def pandas_filter_one():
        return pd.read_parquet(SINGLE_FILE, columns=['pickup_at'], filters=[("pickup_at", ">", datetime(2019, 6, 30))])
    def polars_filter_one():
        return (
            pl.scan_parquet(SINGLE_FILE).select(COLUMNS)
            .filter(pl.col("pickup_at").dt.date() > pl.datetime(2019, 6, 30))
            .collect()
        )
    def duckdb_filter_one():
        return con.execute(f"""
            SELECT pickup_at
            FROM '{SINGLE_FILE}'
            WHERE pickup_at > '2019-06-30'
        """)

    # Filtering function 3 - count rows with filter on pickup_at > '2019-06-30'
    # Note: only selecting one column for optimal row count performance
    def pandas_filter_count():
        return len(pd.read_parquet(SINGLE_FILE, columns=['pickup_at'], filters=[("pickup_at", ">", datetime(2019, 6, 30))]))
    def polars_filter_count():
        return len(
            pl.scan_parquet(SINGLE_FILE)
            .filter(pl.col("pickup_at").dt.date() > pl.datetime(2019, 6, 30))
            .select(["pickup_at"])
            .collect()
        )
    def duckdb_filter_count():
        return con.execute(f"""
            SELECT count(*)
            FROM '{SINGLE_FILE}'
            WHERE pickup_at > '2019-06-30'
        """)

    ### Run benchmarks
    # Benchmark: Read all data (no filter)
    results_all = {
        "Pandas": bm.timing(pandas_read_all, runs=REPEAT_TIMES, label="Pandas"),
        "Polars": bm.timing(polars_read_all, runs=REPEAT_TIMES, label="Polars"),
        "DuckDB": bm.timing(duckdb_read_all, runs=REPEAT_TIMES, label="DuckDB"),
    }

    # Benchmark: Filter column and return all columns
    results_filter_all = {
        "Pandas": bm.timing(pandas_filter_all, runs=REPEAT_TIMES, label="Pandas"),
        "Polars": bm.timing(polars_filter_all, runs=REPEAT_TIMES, label="Polars"),
        "DuckDB": bm.timing(duckdb_filter_all, runs=REPEAT_TIMES, label="DuckDB"),
    }

    # Benchmark: Filter column and return that one column
    results_filter_one = {
        "Pandas": bm.timing(pandas_filter_one, runs=REPEAT_TIMES, label="Pandas"),
        "Polars": bm.timing(polars_filter_one, runs=REPEAT_TIMES, label="Polars"),
        "DuckDB": bm.timing(duckdb_filter_one, runs=REPEAT_TIMES, label="DuckDB"),
    }

    # Benchmark: Filter count
    results_filter_count = {
        "Pandas": bm.timing(pandas_filter_count, runs=REPEAT_TIMES, label="Pandas"),
        "Polars": bm.timing(polars_filter_count, runs=REPEAT_TIMES, label="Polars"),
        "DuckDB": bm.timing(duckdb_filter_count, runs=REPEAT_TIMES, label="DuckDB"),
    }

    ### Plotting results
    # Plot each benchmark group
    bm.plot_results(results_all, f"{TITLE_START}: Read All Data", "speed_read_all_single_file.png")
    bm.plot_results(results_filter_all, f"{TITLE_START}: Filter & Return All Columns", "speed_filter_return_all_cols_single_file.png")
    bm.plot_results(results_filter_one, f"{TITLE_START}: Filter & Return One Column", "speed_filter_return_one_cols_single_file.png")
    bm.plot_results(results_filter_count, f"{TITLE_START}: Filtered Row Count", "speed_filter_count_rows_single_file.png")
    
else:
    ### Functions to compare for reading multiple parquet files
    # Ordinary read functions on all data without filters
    def pandas_read_multi():
        return pd.concat([pd.read_parquet(f) for f in MULTI_FILES])
    def polars_read_multi():
        return pl.read_parquet(MULTI_FILES)
    def duckdb_read_multi():
        return con.execute(f"SELECT * FROM '{MULTI_FILE_PATH}'")
    
    # Filtering function 1 - read ALL columns with filter of pickup month July or older
    def pandas_multi_filter_all():
        df = pd.concat([pd.read_parquet(f) for f in MULTI_FILES])
        return df[df["tpep_pickup_datetime"].dt.month >= 7]
    def polars_multi_filter_all():
        return (
            pl.scan_parquet(MULTI_FILES)
            .filter(pl.col("tpep_pickup_datetime").dt.month() >= 7)
            .collect()
        )
    def duckdb_multi_filter_all():
        return con.execute(f"""
            SELECT *
            FROM '{MULTI_FILE_PATH}'
            WHERE EXTRACT(MONTH FROM tpep_pickup_datetime) >= 7
        """)

    # Filtering function 2 - read ONE column with filter of pickup month July or older
    def pandas_multi_filter_one():
        df = pd.concat([
            pd.read_parquet(f, columns=['tpep_pickup_datetime'])
            for f in MULTI_FILES
        ])
        return df[df["tpep_pickup_datetime"].dt.month >= 7]
    def polars_multi_filter_one():
        return (
            pl.scan_parquet(MULTI_FILES)
            .filter(pl.col("tpep_pickup_datetime").dt.month() >= 7)
            .select(["tpep_pickup_datetime"])
            .collect()
        )
    def duckdb_multi_filter_one():
        return con.execute(f"""
            SELECT tpep_pickup_datetime
            FROM '{MULTI_FILE_PATH}'
            WHERE EXTRACT(MONTH FROM tpep_pickup_datetime) >= 7
        """)

    # Filtering function 3 - count rows with filter of pickup month July or older
    # Note: only selecting one column for optimal row count performance
    def pandas_multi_filter_count():
        return sum([
            len(
                pd.read_parquet(f, columns=['tpep_pickup_datetime'])
                .query("tpep_pickup_datetime.dt.month >= 7")
            )
            for f in MULTI_FILES
        ])
    def polars_multi_filter_count():
        return (
            pl.scan_parquet(MULTI_FILES)
            .filter(pl.col("tpep_pickup_datetime").dt.month() >= 7)
            .select(["tpep_pickup_datetime"])
            .collect()
            .height
        )
    def duckdb_multi_filter_count():
        return con.execute(f"""
            SELECT count(*)
            FROM '{MULTI_FILE_PATH}'
            WHERE EXTRACT(MONTH FROM tpep_pickup_datetime) >= 7
        """)
    
    ### Run benchmarks
    # Benchmark: Read all data (no filter)
    results_all = {
        "Pandas": bm.timing(pandas_read_multi, runs=REPEAT_TIMES, label="Pandas"),
        "Polars": bm.timing(polars_read_multi, runs=REPEAT_TIMES, label="Polars"),
        "DuckDB": bm.timing(duckdb_read_multi, runs=REPEAT_TIMES, label="DuckDB"),
    }

    # Benchmark: Filter column and return all columns
    results_filter_all = {
        "Pandas": bm.timing(pandas_multi_filter_all, runs=REPEAT_TIMES, label="Pandas"),
        "Polars": bm.timing(polars_multi_filter_all, runs=REPEAT_TIMES, label="Polars"),
        "DuckDB": bm.timing(duckdb_multi_filter_all, runs=REPEAT_TIMES, label="DuckDB"),
    }

    # Benchmark: Filter column and return that one column
    results_filter_one = {
        "Pandas": bm.timing(pandas_multi_filter_one, runs=REPEAT_TIMES, label="Pandas"),
        "Polars": bm.timing(polars_multi_filter_one, runs=REPEAT_TIMES, label="Polars"),
        "DuckDB": bm.timing(duckdb_multi_filter_one, runs=REPEAT_TIMES, label="DuckDB"),
    }

    # Benchmark: Filter count
    results_filter_count = {
        "Pandas": bm.timing(pandas_multi_filter_count, runs=REPEAT_TIMES, label="Pandas"),
        "Polars": bm.timing(polars_multi_filter_count, runs=REPEAT_TIMES, label="Polars"),
        "DuckDB": bm.timing(duckdb_multi_filter_count, runs=REPEAT_TIMES, label="DuckDB"),
    }

    ### Plotting results for each benchmark group
    bm.plot_results(results_all, f"{TITLE_START}: Read All Data", "speed_read_all_multiple_files.png")
    bm.plot_results(results_filter_all, f"{TITLE_START}: Filter & Return All Columns", "speed_filter_return_all_cols_multiple_files.png")
    bm.plot_results(results_filter_one, f"{TITLE_START}: Filter & Return One Column", "speed_filter_return_one_cols_multiple_files.png")
    bm.plot_results(results_filter_count, f"{TITLE_START}: Filtered Row Count", "speed_filter_count_rows_multiple_files.png")
