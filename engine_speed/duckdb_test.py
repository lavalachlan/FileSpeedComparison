import duckdb
import pandas as pd
import glob
import pyarrow.dataset as ds
import pyarrow.parquet as pq

con = duckdb.connect()

print(duckdb.query('''
   SELECT *
   FROM 'taxi_2019_04.parquet'
   WHERE pickup_at BETWEEN '2019-04-15' AND '2019-04-20'
   LIMIT 5
   ''').fetchall())

df = pd.concat(
    [pd.read_parquet(file)
     for file
     in glob.glob('taxi*.parquet')])
print(df.head(5))

files = glob.glob("taxi_*.parquet")
dataset = ds.dataset(files, format="parquet")
table = dataset.to_table()
pq.write_table(table, "alltaxi.parquet", row_group_size=100_000)
duckdb.query("SELECT * FROM 'taxi_*.parquet'")
duckdb.query("SELECT * FROM 'alltaxi.parquet'") #does the same thing pretty much, just on combined file

# querying the combined file
con.execute("""
   SELECT *
   FROM 'alltaxi.parquet'
   LIMIT 5""").df()

pd.read_parquet("alltaxi.parquet").head(5)

# counting rows
con.execute("""
   SELECT count(*)
   FROM 'alltaxi.parquet'
""").df()

len(pd.read_parquet('alltaxi.parquet'))
len(pd.read_parquet('alltaxi.parquet', columns=['vendor_id']))

# filtering rows
con.execute("""
   SELECT count(*)
   FROM 'alltaxi.parquet'
   WHERE pickup_at > '2019-06-30'
""").df()

# pandas naive
len(pd.read_parquet('alltaxi.parquet')
          .query("pickup_at > '2019-06-30'"))
# pandas projection pushdown
len(pd.read_parquet('alltaxi.parquet', columns=['pickup_at'])
          .query("pickup_at > '2019-06-30'"))
len(pd.read_parquet('alltaxi.parquet', columns=['pickup_at'], filters=[('pickup_at', '>', '2019-06-30')]))
# read the entire parquet file into Pandas and then filter
df = pd.read_parquet('alltaxi.parquet')
print(len(df[['pickup_at']].query("pickup_at > '2019-06-30'")))

# aggregates
con.execute("""
    SELECT passenger_count, count(*)
    FROM 'alltaxi.parquet'
    GROUP BY passenger_count""").df()
con.from_parquet('alltaxi.parquet')
   .aggregate('passenger_count, count(*)')
   .df()