import pandas as pd
import requests
from sodapy import Socrata
from sqlalchemy import create_engine
import pyodbc

hostname = 'localhost'
database = 'postgres'
username = 'postgres'
pwd = 'R@j@t'
port_id = 5432

engine = create_engine('sqlite://', echo=False)

client = Socrata('data.cityofnewyork.us',
                 'Y52jpbzG7jqL6nARkY66CjzDh')
#                  username="rajat.dahiya@hotmail.com",
#                  password="Rajat$GEE1")
results = client.get("h9gi-nx95", limit=2000)
results_df = pd.DataFrame.from_records(results)
dfNew = results_df.head()
dfNew1 = dfNew.loc[0:5, "on_street_name":"number_of_persons_killed"]
print(dfNew1.dtypes)

# why we used pyodbc instead of psycopd2 because when we were creating the table using psycopd2, it was giving an error of wrong number of arguments

conn = pyodbc.connect('DRIVER={Devart ODBC Driver for PostgreSQL}'  
                      ';Server="localhost"'
                      ';Port=5432'
                      ';Database="postgres"'
                      ';User ID="postgres"'
                      ';Password="R@j@t"'
                      ';String Types=Unicode')

cursor = conn.cursor()

# First we are going to delete existing table
cursor.execute("""DROP TABLE citydata""")

# We will now going to create a table
cursor.execute(
    """CREATE TABLE citydata (on_street_name VARCHAR(30), off_street_name VARCHAR(30),number_of_persons_injured int, number_of_persons_killed int  )""")

# Now in this step, we will going to import json data (fetched from API) to SQL table name citydata
for index, r1 in dfNew1.iterrows():
    cursor.execute(
        "INSERT INTO citydata (on_street_name,off_street_name,number_of_persons_injured,number_of_persons_killed) values(?,?,?,?)",
        r1.on_street_name, r1.off_street_name, r1.number_of_persons_injured, r1.number_of_persons_killed)

# printing
cursor.execute("SELECT * FROM citydata")
row = cursor.fetchone()
while row:
    print(row)
    row = cursor.fetchone()

# just on_street_name
print("-------- 2nd print -------  \n")
cursor.execute("SELECT on_street_name FROM citydata")
row = cursor.fetchone()
while row:
    print(row)
    row = cursor.fetchone()

conn.commit()
conn.close()
