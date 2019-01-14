"""
Author: Jisnu Prabhu
Company: Tod Todd insurance
Date: 01/11/2019
This code loads the flat file and creates and inserts the data into tempory tables. It checks if the duplicates are present.
It deletes the duplicates and inserts the rows to quotes table.
"""
# Packages
from sqlalchemy import create_engine # Connect to database
import mysql.connector
import pandas as pd
from openpyxl import load_workbook # write to excel
from datetime import datetime as dt
import os
import glob # For pattern matching
import time 
import shutil # To move files

# Calculate excute time
start_time = time.time()
#Connecting to the server db
engine = create_engine('mysql+mysqlconnector://root:TTIdb@localhost:3306/sample', echo = False)
conn = engine.connect()

# File to insert to db(Daily Quotes)
file_name = 'C:/Users/jisnu/Desktop/jisnu/data/P&C Total Serious Quotes Detail.xlsx'
#file_name = 'C:/Users/jisnu/Desktop/jisnu/data/fl_quote.xlsx'
df = pd.read_excel(file_name,skiprows = 10)
#df = pd.read_excel(file_name,nrows = 37500)

# File coems from Allstate (bad formatting)
df.drop(['Unnamed: 0','Unnamed: 12'],axis = 1 , inplace = True)
df.drop(df.index[len(df) - 1], axis = 0, inplace = True)
# Changing columns names 
df.columns = [c.lower() for c in df.columns]
df.columns = [c.replace(" ", "_") for c in df.columns]
df.rename(columns={'quote_control_number' : 'quote_number'}, inplace=True)
# Adding zero's to subproducers code (empolyee table has no, code)
df.loc[df.sub_producer == 'No Code', 'sub_producer'] = '000 - No Code'
df['sub_producer'] = df['sub_producer'].str[6:] # Sub code gets recycled 
df[['sales_first_name', 'sales_last_name']] = df['sub_producer'].str.split(" ", 1, True) # link with employee name 
df[['customer_last_name', 'customer_first_name']] = df['customer_name'].str.split(",", 1, True) # can't link, customer not in customer table yet
df.drop(['customer_name', 'sub_producer','agent_number','customer_state','customer_zip_code','channel'], axis = 1 , inplace = True)
df = df[['quote_number','sales_first_name','sales_last_name','customer_first_name','customer_last_name', 'production_date', 'product', 'quoted_item_count', 'quoted_premium']]
# Loads data into a tempory file.
df.to_sql(name = 'fl_quotes_temp', con = engine, if_exists = 'replace', index = False)

conn.execute(
"""
CREATE TABLE IF NOT EXISTS fl_quotes_temp(
	quote_number VARCHAR(50),
	sales_first_name VARCHAR(50),
	sales_last_name VARCHAR(50),
	customer_first_name VARCHAR(50),
	customer_last_name VARCHAR(50),
	production_date VARCHAR(40),
	product VARCHAR(50),
	quoted_item_count INT,
	quoted_premium FLOAT(8,2))
    ENGINE=INNODB;
""")

conn.execute(
"""
INSERT INTO fl_quotes(
	quote_number, license, customer_first_name, customer_last_name, production_date, product ,quoted_item_count ,quoted_premium) 
	SELECT q.quote_number, e.license, q.customer_first_name, q.customer_last_name, q.production_date, q.product , q.quoted_item_count , q.quoted_premium 
	FROM fl_quotes_temp AS q
	LEFT JOIN employee AS e
	ON (e.first_name = q.sales_first_name AND e.last_name = q.sales_last_name)
	WHERE (q.quote_number NOT IN (SELECT quote_number FROM fl_quotes) AND q.production_date NOT IN (SELECT production_date FROM fl_quotes))
	ORDER BY production_date 
""")

conn.execute(""" DROP TABLE fl_quotes_temp; """)

# string with the year+month+day for current day 
"""today = dt.now().date()
if today.day < 10:
  dayNo = '0'+str(today.day)
else:
  dayNo= str(today.day)
day = str(today.year)+str(today.month)+ dayNo

# Moving the file to archive after insertion with the date for the tracking
location_file = "C:/Users/Jisnu/Desktop/jisnu/Extra/P&C Total Serious Quotes Detail_"+day+".xlsx"
shutil.move("C:/Users/Jisnu/Desktop/jisnu/Data/P&C Total Serious Quotes Detail.xlsx", location_file)"""
print('Insert compelete after: {} seconds File inserted and move to Archive!!'.format(time.time() - start_time))

# Deal with dup