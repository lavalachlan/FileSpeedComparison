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
import benchmark

'''
Comparing speeds of Pandas, Polars, and DuckDB against various functions on a Parquet file such as reading and filtering.
This script benchmarks various read and filter operations on a chosen Parquet file using Pandas, Polars, and DuckDB.
It includes functions to read all data, filter data by a date condition, and count rows, as well as miscellaneous
comparisons between Polars LazyFrame and DataFrame, and different Pandas read methods.

This script uses the `benchmark` module to measure the time taken for each operation.

Variables:
- `file`: The Parquet file to be used for benchmarking.
- `repeat_times`: Number of times each function will be run to average the time taken.
- `con`: DuckDB connection object for executing SQL queries.
'''

# init
con = duckdb.connect()

# parameters
file = 'taxi_2019_04.parquet'
repeat_times = 1

### Functions to compare on speeds - for one parquet file 'alltaxi.parquet'
# Ordinary read functions on all data without filters
def pandas_read_all():
    return pd.read_parquet(file)
def polars_read_all():
    return pl.read_parquet(file)
def duckdb_read_all():
    return con.execute(f"SELECT * FROM '{file}'").df()

# Filtering function 1 - read ALL columns with filter on pickup_at > '2019-06-30'
def pandas_filter_all():
    return pd.read_parquet(file, filters=[("pickup_at", ">", datetime(2019, 6, 30))])
def polars_filter_all():
    return pl.read_parquet(file).filter(
        pl.col("pickup_at").dt.date() > pl.datetime(2019, 6, 30))
def duckdb_filter_all():
    return con.execute(f"""
        SELECT *
        FROM '{file}'
        WHERE pickup_at > '2019-06-30'
    """).df()

# Filtering function 2 - read ONE column with filter on pickup_at > '2019-06-30'
def pandas_filter_one():
    return pd.read_parquet(file, columns=['pickup_at'], filters=[("pickup_at", ">", datetime(2019, 6, 30))])
def polars_filter_one():
    return pl.read_parquet(file, columns=["pickup_at"]).filter(
        pl.col("pickup_at").dt.date() > pl.datetime(2019, 6, 30)
    )
def duckdb_filter_one():
    return con.execute(f"""
        SELECT pickup_at
        FROM '{file}'
        WHERE pickup_at > '2019-06-30'
    """).df()

# Filtering function 3 - count rows with filter on pickup_at > '2019-06-30'
# Note: only selecting one column for optimal row count performance
def pandas_filter_count():
    return len(pd.read_parquet(file, columns=['pickup_at'], filters=[("pickup_at", ">", datetime(2019, 6, 30))]))
def polars_filter_count():
    return len(pl.read_parquet(file)
               .filter(pl.col("pickup_at").dt.date() > pl.datetime(2019, 6, 30))
               .select(["pickup_at"]))
def duckdb_filter_count():
    return con.execute(f"""
        SELECT count(*)
        FROM '{file}'
        WHERE pickup_at > '2019-06-30'
    """).df()

## Miscellaneous function comparisons
# Polars LazyFrame vs DataFrame read with filter
# Note: LazyFrame is used for deferred execution, allowing optimizations
def polars_lf_filter_read():
    alltaxi_lf = pl.scan_parquet(file)
    alltaxi_lf_filtered = alltaxi_lf.filter(
        pl.col("pickup_at").dt.date() > pl.datetime(2019, 6, 30)
    ).with_columns(["pickup_at"])
    return alltaxi_lf_filtered.collect()
def polars_df_filter_read():
    alltaxi_df = pl.read_parquet(file)
    alltaxi_df_filtered = alltaxi_df.filter(
        pl.col("pickup_at").dt.date() > pl.datetime(2019, 6, 30)
    ).with_columns(["pickup_at"])
    return alltaxi_df_filtered

# Pandas different read methods
def pandas_read_filter_1():
    alltaxi_pd = pd.read_parquet(file)
    alltaxi_pd_filtered = alltaxi_pd[
        alltaxi_pd["pickup_at"] > datetime(2019, 6, 30)# pd.Timestamp("2019-06-30")
    ][["pickup_at"]]
    return alltaxi_pd_filtered
def pandas_read_filter_2():
    alltaxi_pd = pd.read_parquet(file, filters=[("pickup_at", ">", datetime(2019, 6, 30))])
    return alltaxi_pd[["pickup_at"]]

### Run benchmarks
# Benchmark: Read all data (no filter)
results_all = {
    "Pandas Read All": benchmark.func(pandas_read_all, runs=repeat_times, label="Pandas Read All"),
    "Polars Read All": benchmark.func(polars_read_all, runs=repeat_times, label="Polars Read All"),
    "DuckDB Read All": benchmark.func(duckdb_read_all, runs=repeat_times, label="DuckDB Read All"),
}

# Benchmark: Filter column and return all columns
results_filter_all = {
    "Pandas Filter & Return All Cols": benchmark.func(pandas_filter_all, runs=repeat_times, label="Pandas Filter & Return All Cols"),
    "Polars Filter & Return All Cols": benchmark.func(polars_filter_all, runs=repeat_times, label="Polars Filter & Return All Cols"),
    "DuckDB Filter & Return All Cols": benchmark.func(duckdb_filter_all, runs=repeat_times, label="DuckDB Filter & Return All Cols"),
}

# Benchmark: Filter column and return that one column
results_filter_one = {
    "Pandas Filter & Return One Col": benchmark.func(pandas_filter_one, runs=repeat_times, label="Pandas Filter & Return One Col"),
    "Polars Filter & Return One Col": benchmark.func(polars_filter_one, runs=repeat_times, label="Polars Filter & Return One Col"),
    "DuckDB Filter & Return One Col": benchmark.func(duckdb_filter_one, runs=repeat_times, label="DuckDB Filter & Return One Col"),
}

# Benchmark: Filter count
results_filter_count = {
    "Pandas Filtered Count": benchmark.func(pandas_filter_count, runs=repeat_times, label="Pandas Filtered Count"),
    "Polars Filtered Count": benchmark.func(polars_filter_count, runs=repeat_times, label="Polars Filtered Count"),
    "DuckDB Filtered Count": benchmark.func(duckdb_filter_count, runs=repeat_times, label="DuckDB Filtered Count"),
}

# Benchmark: Miscellaneous function comparisons
results_misc = {
    "Polars LazyFrame Filter Read": benchmark.func(polars_lf_filter_read, runs=repeat_times, label="Polars LazyFrame Filter Read"),
    "Polars DataFrame Filter Read": benchmark.func(polars_df_filter_read, runs=repeat_times, label="Polars DataFrame Filter Read"),
    "Pandas Read Filter 1": benchmark.func(pandas_read_filter_1, runs=repeat_times, label="Pandas Read Filter 1"),
    "Pandas Read Filter 2": benchmark.func(pandas_read_filter_2, runs=repeat_times, label="Pandas Read Filter 2"),
}

### Plotting results

# Helper function to plot results
def plot_results(results_dict, title, filename):
    df_plot = pd.DataFrame([
        {"Method": key, "Time (s)": value}
        for key, value in results_dict.items()
    ])
    print(df_plot)
    plt.figure(figsize=(6,4))
    ax = sns.barplot(df_plot, x="Method", y="Time (s)", palette="pastel")
    for container in ax.containers:
        ax.bar_label(container, fmt="%.4f", fontsize=10)
    plt.title(title)
    plt.tight_layout()
    plt.savefig(filename)
    plt.show()

# Plot each benchmark group
plot_results(results_all, "Benchmark: Read All Data", "speed_read_all.png")
plot_results(results_filter_all, "Benchmark: Filter & Return All Columns", "speed_filter_all.png")
plot_results(results_filter_one, "Benchmark: Filter & Return One Column", "speed_filter_one.png")
plot_results(results_filter_count, "Benchmark: Filtered Row Count", "speed_filter_count.png")
plot_results(results_misc, "Benchmark: Miscellaneous Comparisons", "speed_misc.png")