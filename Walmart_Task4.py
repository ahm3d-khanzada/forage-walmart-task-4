import pandas as pd
import sqlite3 as sql

# Reading the CSV Files
df0 = pd.read_csv('forage-walmart-task-4/data/shipping_data_0.csv')
df1 = pd.read_csv('forage-walmart-task-4/data/shipping_data_1.csv')
df2 = pd.read_csv('forage-walmart-task-4/data/shipping_data_2.csv')
# print(df0) #For Testing purpose

# Combine the data
combine_data = pd.merge(df1, df2 , on='shipment_identifier')
# print(combine_data.head)

combine_data['Quantity'] = combine_data.groupby('shipment_identifier')['product'].transform('count')
# print(combine_data.head)


#Makeing Connection with sqlite
connection = sql.connect('forage-walmart-task-4/shipment_database.db',timeout=10)
fetch = connection.cursor()

fetch.execute('''
CREATE TABLE IF NOT EXISTS shipments (
    ShippingID TEXT,
    Origin TEXT,
    Destination TEXT,
    ProductID TEXT,
    Quantity INTEGER,
    OnTime BOOLEAN,
    DriverID TEXT
)
''')

# Insert Data of CSV0 in DB
for index, row in df0.iterrows():
    fetch.execute('''
    INSERT INTO shipments (Origin, Destination, ProductID, Quantity, OnTime, DriverID) 
    VALUES ( ?, ?, ?, ?, ?, ?)''', 
    (row['origin_warehouse'], row['destination_store'], 
     row['product'], row['product_quantity'], row['on_time'], row['driver_identifier']))
     
connection.commit()

# Insert the data of combine Data df1 and df2 in DB

for index, row in combine_data.iterrows():
    fetch.execute('''
                  INSERT INTO shipments (ShippingID,Origin, Destination, ProductID, OnTime, DriverID) 
    VALUES ( ?, ?, ?, ?, ?, ?)''',
    (row['origin_warehouse'], row['destination_store'], 
     row['product'], row['driver_identifier'], row['on_time'],row['shipment_identifier']))
    
    connection.commit()
    connection.close

