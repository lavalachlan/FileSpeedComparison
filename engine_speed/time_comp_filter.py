import pandas as pd
import time
import pyarrow as pa
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import duckdb
import glob
import pyarrow.dataset as ds
import pyarrow.parquet as pq

'''
Comparing speeds of reading parquet data with filters using pandas and duckdb.
'''

# init
con = duckdb.connect()

# parameters
repeat_times = 5

# Benchmarking function
def benchmark(func, runs=repeat_times, label=""):
    times = []
    for _ in range(runs):
        start = time.perf_counter()
        func()
        end = time.perf_counter()
        times.append(end - start)
    avg_time = np.mean(times)
    print(f"{label:<15} | Avg: {avg_time:.6f}s over {runs} runs")
    return avg_time

# Filtering functions to perform
def pandas_filter():
    return len(pd.read_parquet('alltaxi.parquet', columns=['pickup_at'])
               .query("pickup_at > '2019-06-30'"))

def duckdb_filter():
    return con.execute("""
        SELECT count(*)
        FROM 'alltaxi.parquet'
        WHERE pickup_at > '2019-06-30'
    """).df()

# Run benchmarks
results = {
    "DuckDB Filter": benchmark(duckdb_filter, label="DuckDB Filter"),
    "Pandas Filter": benchmark(pandas_filter, label="Pandas Filter")
}

df_plot = pd.DataFrame([
    {"Method": key, "Time (s)": value}
    for key, value in results.items()
])
print(df_plot)

# Plotting
sns.set(style="whitegrid")

plt.figure(figsize=(6,4))
ax = sns.barplot(df_plot, x="Method", y="Time (s)", palette="pastel")

for container in ax.containers:
    ax.bar_label(container, fmt="%.4f", fontsize=10)

plt.title("Benchmark: DuckDB vs Pandas Filtering")
plt.tight_layout()
plt.savefig('speed_filter_comparisons.png')
plt.show()
