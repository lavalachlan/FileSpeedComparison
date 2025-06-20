import pandas as pd
import time
import pyarrow as pa
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import benchmark

'''
Comparing read and write speeds of csv and parquet file types using PANDAS.
The PyArrow engine is used for handling the parquet data.
'''

# Read the CSV file in chunks
chunksize = 10**6  # Adjust the chunk size as needed
csv_file = '5m-Sales-Records/5m Sales Records.csv'
# https://excelbianalytics.com/wp/downloads-18-sample-csv-files-data-sets-for-testing-sales/

# Initialize an empty list to store chunks
chunks = []
for chunk in pd.read_csv(csv_file, chunksize=chunksize):
    chunks.append(chunk)
df = pd.concat(chunks)
# print(df.head())
# print(df.describe())

# OR Generate dummy data - data if written 
size = 10_000_000
df = pd.DataFrame({
    "A": np.random.randint(0, 1000, size=size),
    "B": np.random.rand(size),
    "C": np.random.choice(["X", "Y", "Z"], size=size)
})

# Initial function to write to csv and parquet - can comment out when files are present in dir
# df.to_csv('Sales.csv')
# df.to_parquet('Sales.parquet', engine='pyarrow')

### ChatGPT example ###

# Run benchmarks
results_write = {
    "CSV Write": benchmark.func(df.to_csv, "Sales.csv", index=False, label="CSV Write"),
    "Parquet Write": benchmark.func(df.to_parquet, "Sales.parquet", engine="pyarrow", label="Parquet Write")
}
results_read = {
    "CSV Read": benchmark.func(pd.read_csv, "Sales.csv", label="CSV Read"),
    "Parquet Read": benchmark.func(pd.read_parquet, "Sales.parquet", engine="pyarrow", label="Parquet Read"),
}

# Table output
combined_results = results_write | results_read
df_results = pd.DataFrame.from_dict(combined_results, orient='index', columns=['Average Time (s)'])
print(df_results)

# Plotting
def prepare_df(results_dict):
    # Extract format and time into a DataFrame
    data = []
    for k, v in results_dict.items():
        fmt = k.split()[0]  # CSV or Parquet
        data.append({"Format": fmt, "Time (s)": v})
    return pd.DataFrame(data)

df_write = prepare_df(results_write)
df_read = prepare_df(results_read)

sns.set(style="whitegrid")

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 4), sharey=False)

sns.barplot(data=df_write, x="Format", y="Time (s)", palette=["tab:blue", "tab:orange"], ax=ax1)
ax1.set_title("Write Benchmark: CSV vs Parquet")
ax1.bar_label(ax1.containers[0], fmt="%.3f")

sns.barplot(data=df_read, x="Format", y="Time (s)", palette=["tab:blue", "tab:orange"], ax=ax2)
ax2.set_title("Read Benchmark: CSV vs Parquet")
ax2.bar_label(ax2.containers[0], fmt="%.3f")

plt.tight_layout()
plt.savefig('speed_comparisons.png')
plt.show()
