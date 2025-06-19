print('Hello World')

import pandas as pd
import numpy as np

Sales_csv = pd.read_csv('hello/5m-Sales-Records/5m Sales Records.csv')

import sys
print(sys.version)

series = pd.Series(2*np.arange(10))
series2 = pd.Series([0,1,2,3,4,5])
print(series, type(series))
print(series2, type(series2))

dates = pd.date_range("20250601",periods=30)
print(dates, type(dates))

df = pd.DataFrame(np.random.randn(10,6), index=series, columns=list("hello."))
print(df)

arr1 = np.random.randn(10, 2)
arr2 = np.random.rand(10, 2)
chars = list("testing123")  # 10 characters
df = pd.DataFrame({
    "hello": [a for a in arr1[:1]],    # list of arrays (2,)
    "world": chars,                # single characters
    "test":  [b for b in arr2]     # list of arrays (2,)
})
print(df)
print(df.head())

df.boxplot()

df.tail(3)
df.index
df.to_numpy()

df2 = pd.DataFrame(
    {
        "A": 1.0,
        "B": pd.Timestamp("20130102"),
        "C": pd.Series(1, index=list(range(4)), dtype="float32"),
        "D": np.array([3] * 4, dtype="int32"),
        "E": pd.Categorical(["test", "train", "test", "train"]),
        "F": "foo",
    }
)
df2
df2.dtypes
df2.columns
df2.to_numpy()
df2.describe()
df2.T
df2.sort_index(axis=1, ascending=False) # sorts by either row or column index
df.sort_values(by="world") # sorts by column values

# Getitem []
df2["C"]
df["hello"]
df[0:2]
# Selection by label
df.loc[1]
df2.loc[df2['A']==1]
df2.loc[:,["A","B"]]
# Selection by position
df2.iloc[1:5,1:4]
# Boolean
df2[df2>0.5]

# User defined functions
df3 = pd.DataFrame(np.random.randn(5,4))
df3
df3.agg(lambda x: np.sum(np.ceil(x**2)))
df3.transform(lambda x: np.ceil(x**10))
df3[1].value_counts()

# Joins
left = pd.DataFrame({"key": ["foo", "bar"], "lval": [1, 2]})
right = pd.DataFrame({"key": ["foo", "bar"], "rval": [4, 5]})
left
right
pd.merge(left, right, on="key")

# Plotting
import matplotlib.pyplot as plt
plt.close("all")
ts = pd.Series(np.random.randn(1000), index=pd.date_range("1/1/2002", periods=1000))
ts = ts.cumsum()
plt.plot(ts)
plt.show()

# Files
import pyarrow as pa
df2.to_parquet('hello/example_parquet.parquet')
example_parquet = pd.read_parquet('hello/example_parquet.parquet')
example_parquet