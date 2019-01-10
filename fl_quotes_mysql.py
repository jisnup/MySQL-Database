from sqlalchemy import create_engine
import mysql.connector
import pandas as pd
from openpyxl import load_workbook
from datetime import datetime as dt
import os
import glob
import time
import shutil

start_time = time.time()

engine = create_engine('mysql+mysqlconnector://root:TTIdb@localhost:3306/sample', echo = False)
conn = engine.connect()

# P&C Total Serious Quotes Detail
file_name = 'C:/Users/jisnu/Desktop/jisnu/data/P&C Total Serious Quotes Detail.xlsx'
df = pd.read_excel(file_name,skiprows = 10)
#file_name = 'C:/Users/jisnu/Desktop/jisnu/data/fl_quote.xlsx'
#df = pd.read_excel(file_name,nrows = 37500)

#df.drop(['Unnamed: 0','Unnamed: 12'],axis = 1 , inplace = True)
#df.drop(df.index[len(df) - 1], axis = 0, inplace = True)

df.columns = [c.lower() for c in df.columns]
df.columns = [c.replace(" ", "_") for c in df.columns]

df.drop(['agent_number','customer_state','customer_zip_code','channel'], axis =1, inplace = True)
df.rename(columns={'quote_control_number' : 'quote_number'}, inplace=True)
df.loc[df.sub_producer == 'No Code', 'sub_producer'] = '000 - No Code'
df['sub_producer'] = df['sub_producer'].str[6:]
df[['sales_first_name', 'sales_last_name']] = df['sub_producer'].str.split(" ", 1, True)
df[['customer_last_name', 'customer_first_name']] = df['customer_name'].str.split(",", 1, True)
df.drop(['customer_name', 'sub_producer'], axis = 1 , inplace = True)
df = df[['quote_number','sales_first_name','sales_last_name','customer_first_name','customer_last_name', 'production_date', 'product', 'quoted_item_count', 'quoted_premium']]

df.to_sql(name = 'fl_quotes_temp', con = engine, if_exists = 'append', index = False)

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
DELETE FROM fl_quotes_temp
	WHERE quote_number NOT IN (
	SELECT quote_number FROM (
	fl_quotes_temp AS f
	INNER JOIN fl_quotes AS c
	WHERE f.quote_number = c.quote_number AND f.production_date = c.production_date)

	WHERE category_id NOT IN (
    SELECT cid FROM (
        SELECT DISTINCT category.id AS cid FROM category 
        INNER JOIN story_category ON category_id=category.id
    ) AS c
)
""")

conn.execute(
"""
INSERT INTO fl_quotes(
	quote_number, license, customer_first_name, customer_last_name, production_date, product ,quoted_item_count ,quoted_premium) 
	SELECT q.quote_number, e.license, q.customer_first_name, q.customer_last_name, q.production_date, q.product , q.quoted_item_count , q.quoted_premium 
	FROM fl_quotes_temp AS q
	JOIN employee AS e
	WHERE (e.first_name = q.sales_first_name AND e.last_name = q.sales_last_name) 
""")

conn.execute(""" DROP TABLE fl_quotes_temp; """)

d = dt.now().date()
if d.day < 10:
  dd = '0'+str(d.day)
else:
  dd = str(d.day)
day = str(d.year)+str(d.month)+dd

"""
new_file = "C:/Users/Jisnu/Desktop/jisnu/Extra/business_mysql_"+day+".py"
shutil.move("C:/Users/Jisnu/Desktop/jisnu/Data/business_mysql.py", new_file)
print('File inserted and move to Archive!!')
"""
print('Insert compelete after: {} seconds'.format(time.time() - start_time))

# Deal with dup