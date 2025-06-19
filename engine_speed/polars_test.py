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

# filter alltaxi.parquet by pickup_at > '2019-06-30'
alltaxi_df = pl.read_parquet('alltaxi.parquet')
alltaxi_df_filtered = alltaxi_df.filter(
    pl.col("pickup_at").dt.date() > pl.datetime(2019, 6, 30)
    ).with_columns(
        ["vendor_id","pickup_at", "total_amount"]
    ).limit(10)
print(alltaxi_df)

alltaxi_lf = pl.scan_parquet('alltaxi.parquet')
alltaxi_lf_filtered = alltaxi_lf.filter(
    pl.col("pickup_at").dt.date() > pl.datetime(2019, 6, 30)
    ).with_columns(
        ["vendor_id","pickup_at", "total_amount"]
    ).limit(10)
# print(filtered_scan.show_graph())
print(alltaxi_lf_filtered.collect())

alltaxi_pd = pd.read_parquet('alltaxi.parquet')
alltaxi_pd_filtered = alltaxi_pd[
    alltaxi_pd["pickup_at"] > pd.Timestamp("2019-06-30")
][["vendor_id", "pickup_at", "total_amount"]].head(10)
print(alltaxi_pd_filtered)
