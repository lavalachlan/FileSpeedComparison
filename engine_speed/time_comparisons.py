from warnings import filters
import pandas as pd
import polars as pl
import duckdb
from datetime import datetime
# import time
import pyarrow as pa
# import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
# import glob
# import pyarrow.dataset as ds
# import pyarrow.parquet as pq

# Benchmark .py file
import benchmark as bm

'''
Comparing speeds of Pandas, Polars, and DuckDB against various functions on a Parquet file such as reading and filtering.
This script benchmarks various read and filter operations on a chosen Parquet file using Pandas, Polars, and DuckDB.
It includes functions to read all data, filter data by a date condition, and count rows, as well as miscellaneous
comparisons between Polars LazyFrame and DataFrame, and different Pandas read methods.

This script uses the `benchmark` module to measure the time taken for each operation and plot the results.

Variables:
- `FILE`: The Parquet file to be used for benchmarking.
- `REPEAT_TIMES`: Number of times each function will be run to average the time taken.
- `con`: DuckDB connection object for executing SQL queries.
'''

# parameters
FILE = 'data/taxi_2019_04.parquet'
REPEAT_TIMES = 1

# init
con = duckdb.connect()

### Functions to compare on speeds - for one parquet file 'alltaxi.parquet'
# Ordinary read functions on all data without filters
def pandas_read_all():
    return pd.read_parquet(FILE)
def polars_read_all():
    return pl.read_parquet(FILE)
def duckdb_read_all():
    return con.execute(f"SELECT * FROM '{FILE}'").df()

# Filtering function 1 - read ALL columns with filter on pickup_at > '2019-06-30'
def pandas_filter_all():
    return pd.read_parquet(FILE, filters=[("pickup_at", ">", datetime(2019, 6, 30))])
def polars_filter_all():
    return pl.read_parquet(FILE).filter(
        pl.col("pickup_at").dt.date() > pl.datetime(2019, 6, 30))
def duckdb_filter_all():
    return con.execute(f"""
        SELECT *
        FROM '{FILE}'
        WHERE pickup_at > '2019-06-30'
    """).df()

# Filtering function 2 - read ONE column with filter on pickup_at > '2019-06-30'
def pandas_filter_one():
    return pd.read_parquet(FILE, columns=['pickup_at'], filters=[("pickup_at", ">", datetime(2019, 6, 30))])
def polars_filter_one():
    return pl.read_parquet(FILE, columns=["pickup_at"]).filter(
        pl.col("pickup_at").dt.date() > pl.datetime(2019, 6, 30)
    )
def duckdb_filter_one():
    return con.execute(f"""
        SELECT pickup_at
        FROM '{FILE}'
        WHERE pickup_at > '2019-06-30'
    """).df()

# Filtering function 3 - count rows with filter on pickup_at > '2019-06-30'
# Note: only selecting one column for optimal row count performance
def pandas_filter_count():
    return len(pd.read_parquet(FILE, columns=['pickup_at'], filters=[("pickup_at", ">", datetime(2019, 6, 30))]))
def polars_filter_count():
    return len(pl.read_parquet(FILE)
               .filter(pl.col("pickup_at").dt.date() > pl.datetime(2019, 6, 30))
               .select(["pickup_at"]))
def duckdb_filter_count():
    return con.execute(f"""
        SELECT count(*)
        FROM '{FILE}'
        WHERE pickup_at > '2019-06-30'
    """).df()

## Miscellaneous function comparisons
# Polars LazyFrame vs DataFrame read with filter
# Note: LazyFrame is used for deferred execution, allowing optimizations
def polars_lf_filter_read():
    alltaxi_lf = pl.scan_parquet(FILE)
    alltaxi_lf_filtered = alltaxi_lf.filter(
        pl.col("pickup_at").dt.date() > pl.datetime(2019, 6, 30)
    ).with_columns(["pickup_at"])
    return alltaxi_lf_filtered.collect()
def polars_df_filter_read():
    alltaxi_df = pl.read_parquet(FILE)
    alltaxi_df_filtered = alltaxi_df.filter(
        pl.col("pickup_at").dt.date() > pl.datetime(2019, 6, 30)
    ).with_columns(["pickup_at"])
    return alltaxi_df_filtered

# Pandas different read methods
def pandas_read_filter_1():
    alltaxi_pd = pd.read_parquet(FILE)
    alltaxi_pd_filtered = alltaxi_pd[
        alltaxi_pd["pickup_at"] > datetime(2019, 6, 30)# pd.Timestamp("2019-06-30")
    ][["pickup_at"]]
    return alltaxi_pd_filtered
def pandas_read_filter_2():
    alltaxi_pd = pd.read_parquet(FILE, filters=[("pickup_at", ">", datetime(2019, 6, 30))])
    return alltaxi_pd[["pickup_at"]]

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

# Benchmark: Miscellaneous function comparisons
results_misc = {
    "Polars LazyFrame": bm.timing(polars_lf_filter_read, runs=REPEAT_TIMES, label="Polars LazyFrame"),
    "Polars DataFrame": bm.timing(polars_df_filter_read, runs=REPEAT_TIMES, label="Polars DataFrame"),
    "Pandas Read Filter 1": bm.timing(pandas_read_filter_1, runs=REPEAT_TIMES, label="Pandas Read Filter 1"),
    "Pandas Read Filter 2": bm.timing(pandas_read_filter_2, runs=REPEAT_TIMES, label="Pandas Read Filter 2"),
}

### Plotting results
# Plot each benchmark group
bm.plot_results(results_all, "Benchmark: Read All Data", "speed_read_all.png")
bm.plot_results(results_filter_all, "Benchmark: Filter & Return All Columns", "speed_filter_all.png")
bm.plot_results(results_filter_one, "Benchmark: Filter & Return One Column", "speed_filter_one.png")
bm.plot_results(results_filter_count, "Benchmark: Filtered Row Count", "speed_filter_count.png")
bm.plot_results(results_misc, "Benchmark: Miscellaneous Comparisons", "speed_misc.png")