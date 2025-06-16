import pandas as pd
import time

# Read the CSV file in chunks
chunksize = 10**6  # Adjust the chunk size as needed
csv_file = '5m-Sales-Records/5m Sales Records.csv'
# parquet_file = 'large_dataset.parquet'

# Initialize an empty list to store chunks
chunks = []

for chunk in pd.read_csv(csv_file, chunksize=chunksize):
    chunks.append(chunk)

# Concatenate all chunks into a single DataFrame
start = time.perf_counter()
df = pd.concat(chunks)
end = time.perf_counter()
print(f"Time for CSV: {end-start:.6f} seconds")

print(df.head())

# Write the DataFrame to a Parquet file
# df.to_parquet(parquet_file, engine='pyarrow')

print(df.head())
