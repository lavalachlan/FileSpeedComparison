from datetime import datetime
import pandas as pd
import polars as pl
import benchmark as bm

from datetime import datetime
import pandas as pd
import polars as pl
import benchmark as bm

FILE = "data/taxi_2019_04.parquet"
FILTER = [("pickup_at", ">", datetime(2019, 6, 30))]
COLUMNS = ["pickup_at", "total_amount"]
TITLE_START = "1 File Read Benchmark"
SHOW_PLOTS = False
REPEAT_TIMES = 5

df = pl.read_parquet(FILE).filter(pl.col("pickup_at") > datetime(2019, 6, 30)).select(COLUMNS)
lf = pl.scan_parquet(FILE).filter(pl.col("pickup_at") > datetime(2019, 6, 30)).select(COLUMNS)

# Polars sink vs write parquet
def pl_read():
    df = pl.read_parquet(FILE).filter(pl.col("pickup_at") > datetime(2019, 6, 30)).select(COLUMNS)
    return df

def pl_scan():
    lf = pl.scan_parquet(FILE).filter(pl.col("pickup_at") > datetime(2019, 6, 30)).select(COLUMNS)
    return lf

def pl_write():
    df.write_parquet("data/test.parquet")

def pl_sink():
    lf.sink_parquet("data/test.parquet")

# Benchmarks
methods_sink_write = {
    "Read": pl_read,
    "Scan": pl_scan,
    "Write": pl_write,
    "Sink": pl_sink
}

results_sink_write = bm.run_benchmark(methods_sink_write, runs=REPEAT_TIMES)

# Plotting the results
bm.plot_results(results_sink_write, f"{TITLE_START}: Polars Sink vs Write Methods (With Filters)", "speed_polars_sink_write_with_filters.png")