"""
Sample script to bulk load data into SQL Server

Please read the comments and update as needed for your test case
"""

import pandas as pd
from sqlalchemy.engine import create_engine, URL
import time

# Change to your source data file path
source_file = 'C:\\Test\\Real_Estate_Sales_2001-2020_GL.csv'

# Change username, password, host, and database to the values required to connect to your database
connect_url = URL.create(
    'mssql+pyodbc',
    username="test",
    password="1234",
    host=".",
    database="DA_Dev",
    query=dict(driver='ODBC Driver 17 for SQL Server'))

engine = create_engine(
    url=connect_url,
    fast_executemany=True,
)

# Test SQL Server Connection
try:
    with engine.connect() as connection:
        print("Connection successful!")
except Exception as e:
    print(f"Error occurred: {e}")

# Load Dataset
df = pd.read_csv (source_file, low_memory=False)
start = time.time()
df.to_sql(con=engine, schema="dbo", name="CT_Real_Estate", if_exists="replace", index=False, chunksize=1000)
end = time.time()
print(end - start)