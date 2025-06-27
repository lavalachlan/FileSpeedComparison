from datetime import datetime
import pandas as pd
import polars as pl
import benchmark as bm

FILE = "data/taxi_2019_04.parquet"
FILTER = [("pickup_at", ">", datetime(2019, 6, 30))]
COLUMNS = ["pickup_at", "total_amount"]  # subset for projection
TITLE_START = "1 File Read Benchmark"
SHOW_PLOTS = False
REPEAT_TIMES = 5

### Pushdown comparisons for Pandas and Polars

## Pandas pushdown methods
# --- Full scan: read everything, filter after
def pd_read_all_then_filter():
    df = pd.read_parquet(FILE)
    return df[df["pickup_at"] > pd.Timestamp("2019-06-30")][COLUMNS]

# --- Projection pushdown only
def pd_read_columns_then_filter():
    df = pd.read_parquet(FILE, columns=COLUMNS)
    return df[df["pickup_at"] > pd.Timestamp("2019-06-30")]

# --- Filter pushdown (pyarrow backend)
def pd_read_with_filter_pushdown():
    return pd.read_parquet(FILE, engine="pyarrow", filters=FILTER)[COLUMNS]

# --- Filter + projection pushdown
def pd_read_with_filter_and_projection():
    return pd.read_parquet(FILE, engine="pyarrow", columns=COLUMNS, filters=FILTER)

## Polars pushdown methods
# Polars dataframes
# --- Full scan: read everything, filter after
def pl_read_all_then_filter():
    df = pl.read_parquet(FILE)
    return df.filter(pl.col("pickup_at") > datetime(2019, 6, 30)).select(COLUMNS)

# --- Projection pushdown only
def pl_read_columns_then_filter():
    df = pl.read_parquet(FILE, columns=COLUMNS)
    return df.filter(pl.col("pickup_at") > datetime(2019, 6, 30))

# --- Polars DataFrame cannot do filter pushdown directly like Pandas


# Polars LazyFrames
def pl_lazy_read_all_then_filter():
    lf = pl.scan_parquet(FILE)
    return lf.collect().filter(pl.col("pickup_at") > datetime(2019, 6, 30)).select(COLUMNS)

def pl_lazy_read_columns_then_filter():
    lf = pl.scan_parquet(FILE).select(COLUMNS)
    return lf.collect().filter(pl.col("pickup_at") > datetime(2019, 6, 30))

def pl_lazy_read_with_filter_pushdown():
    lf = pl.scan_parquet(FILE).filter(pl.col("pickup_at") > datetime(2019, 6, 30))
    return lf.collect().select(COLUMNS)

def pl_lazy_read_with_filter_and_projection():
    lf = (
        pl.scan_parquet(FILE)
        .filter(pl.col("pickup_at") > datetime(2019, 6, 30))
        .select(COLUMNS)
    )
    return lf.collect()

# Benchmarks
methods_pushdown_pd = {
    "Full Scan (Pandas)": pd_read_all_then_filter,
    "Projection Pushdown (Pandas)": pd_read_columns_then_filter,
    "Filter Pushdown (Pandas)": pd_read_with_filter_pushdown,
    "Filter And Projection Pushdown (Pandas)": pd_read_with_filter_and_projection,
}

methods_pushdown_pl_df = {
    "Full Scan (Polars DataFrame)": pl_read_all_then_filter,
    "Projection Pushdown (Polars DataFrame)": pl_read_columns_then_filter,
}
methods_pushdown_pl_lf = {
    "Full Scan (Polars LazyFrame)": pl_lazy_read_all_then_filter,
    "Projection Pushdown (Polars LazyFrame)": pl_lazy_read_columns_then_filter,
    "Filter Pushdown (Polars LazyFrame)": pl_lazy_read_with_filter_pushdown,
    "Filter And Projection Pushdown (Polars LazyFrame)": pl_lazy_read_with_filter_and_projection,
}

results_pushdown_pd = bm.run_benchmark(methods_pushdown_pd, runs=REPEAT_TIMES)
results_pushdown_relative_pd = bm.relative_speeds(results_pushdown_pd)

results_pushdown_pl_df = bm.run_benchmark(methods_pushdown_pl_df, runs=REPEAT_TIMES)
results_pushdown_pl_df_relative = bm.relative_speeds(results_pushdown_pl_df)

results_pushdown_pl_lf = bm.run_benchmark(methods_pushdown_pl_lf, runs=REPEAT_TIMES)
results_pushdown_pl_lf_relative = bm.relative_speeds(results_pushdown_pl_lf)

results_combined = {**results_pushdown_pd, **results_pushdown_pl_df, **results_pushdown_pl_lf}

# Plotting the results
bm.plot_results(results_pushdown_pd, f"{TITLE_START}: Pushdown Methods (Pandas)", "speed_pushdown_methods_pandas.png")
bm.plot_results(results_pushdown_relative_pd, f"{TITLE_START}: Pushdown Methods (Pandas)", relative=True, show=SHOW_PLOTS)

bm.plot_results(results_pushdown_pl_df, f"{TITLE_START}: Pushdown Methods (Polars DataFrame)", "speed_pushdown_methods_polars.png")
bm.plot_results(results_pushdown_pl_df_relative, f"{TITLE_START}: Pushdown Methods (Polars DataFrame)", relative=True, show=SHOW_PLOTS)

bm.plot_results(results_pushdown_pl_lf, f"{TITLE_START}: Pushdown Methods (Polars LazyFrame)", "speed_pushdown_methods_polars_lazy.png")
bm.plot_results(results_pushdown_pl_lf_relative, f"{TITLE_START}: Pushdown Methods (Polars LazyFrame)", relative=True, show=SHOW_PLOTS)

bm.plot_results(results_combined, f"{TITLE_START}: Pushdown Methods Combined", "speed_pushdown_methods.png", xtick_rotation=30)