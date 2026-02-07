import sys
import pandas as pd
sys.argv  # to  take arguments into python file.

#print('NYC Pipeline')

month = int(sys.argv[1])

data = {
    "ID": [1, 2, 3, 4],
    "Name": ["Alice", "Bob", "Charlie", "David"],
    "Age": [25, 30, 35, 40],
    "City": ["New York", "Los Angeles", "Chicago", "Houston"]
}
df = pd.DataFrame(data)

print(df.head())

df.to_parquet(f"output_{month}.parquet")

print("Month: " , month)
