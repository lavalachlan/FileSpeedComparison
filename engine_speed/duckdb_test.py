import duckdb
import pandas as pd
import glob
import pyarrow.dataset as ds
import pyarrow.parquet as pq

con = duckdb.connect()

print(duckdb.query('''
   SELECT *
   FROM 'data/taxi_2019_04.parquet'
   WHERE pickup_at BETWEEN '2019-04-15' AND '2019-04-20'
   LIMIT 5
   ''').fetchall())

df = pd.concat(
    [pd.read_parquet(file)
     for file
     in glob.glob('data/taxi/yellow*.parquet')])
print(df.head(5))

files = glob.glob("data/taxi/yellow*.parquet")
dataset = ds.dataset(files, format="parquet")
table = dataset.to_table()
pq.write_table(table, "data/alltaxi.parquet", row_group_size=100_000)
duckdb.query("SELECT * FROM 'data/taxi_*.parquet'")
duckdb.query("SELECT * FROM 'data/alltaxi.parquet'") #does the same thing pretty much, just on combined file

# querying the combined file
con.execute("""
   SELECT *
   FROM 'data/alltaxi.parquet'
   LIMIT 5""").df()

pd.read_parquet("data/alltaxi.parquet").head(5)

# counting rows
con.execute("""
   SELECT count(*)
   FROM 'data/alltaxi.parquet'
""").df()

len(pd.read_parquet('data/alltaxi.parquet'))
len(pd.read_parquet('data/alltaxi.parquet', columns=['vendor_id']))

# filtering rows
con.execute("""
   SELECT count(*)
   FROM 'data/alltaxi.parquet'
   WHERE pickup_at > '2019-06-30'
""").df()

# pandas naive
len(pd.read_parquet('data/alltaxi.parquet')
          .query("pickup_at > '2019-06-30'"))
# pandas projection pushdown
len(pd.read_parquet('data/alltaxi.parquet', columns=['pickup_at'])
          .query("pickup_at > '2019-06-30'"))
len(pd.read_parquet('data/alltaxi.parquet', columns=['pickup_at'], filters=[('pickup_at', '>', '2019-06-30')]))
# read the entire parquet file into Pandas and then filter
df = pd.read_parquet('data/alltaxi.parquet')
print(len(df[['pickup_at']].query("pickup_at > '2019-06-30'")))

# aggregates
con.execute("""
    SELECT passenger_count, count(*)
    FROM 'data/alltaxi.parquet'
    GROUP BY passenger_count""").df()
con.from_parquet('data/alltaxi.parquet')
   .aggregate('passenger_count, count(*)')
   .df()
   
# testing to see if df is returned without .df() after duckdb query
SINGLE_FILE = 'data/taxi_2019_04.parquet'
MULTI_FILE_PATH = "data/taxi/*01.parquet"
MULTI_FILES = glob.glob(MULTI_FILE_PATH)

print(con.execute(f"SELECT * FROM '{SINGLE_FILE}'"))
print(con.execute(f"SELECT * FROM '{SINGLE_FILE}'").df())
print(con.execute(f"SELECT * FROM '{SINGLE_FILE}'").fetchdf())