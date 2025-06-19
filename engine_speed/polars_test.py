import polars as pl
import pandas as pd

# taxi_pl = pl.read_parquet('alltaxi.parquet')
# taxi_pd = pd.read_parquet('alltaxi.parquet')
dummy_pl = pl.read_csv('dummy.csv', has_header=True)
dummy_pd = pd.read_csv('dummy.csv', header=0)

# print("Polars Taxi DataFrame:", taxi_pl.head())
# print("Pandas Taxi DataFrame:", taxi_pd.head())
print("Polars Dummy DataFrame:", dummy_pl.head())
print("Pandas Dummy DataFrame:", dummy_pd.head())

print(f"{type(dummy_pd) = } | {type(dummy_pl) = }")
print(f"{dummy_pl.shape = } | {dummy_pl.columns = }")

dummy_pd["A"]
dummy_pl.get_column("A")

dummy_pl[:10, ["A", "B"]]
dummy_pl.limit(10).select(["A", "B"])