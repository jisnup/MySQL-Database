"""
Author: Jisnu Prabhu
Company: Tod Todd insurance
Date: 01/11/2019
This code loads the flat file and creates and inserts the data into temporary tables. It checks if the duplicates are present.
It deletes the duplicates and inserts the unique rows to sales table. There are five tables, three for each transaction type and two for each disposition code
New view has to be created for getting the up to date information about the policy 
"""
# Packages
from sqlalchemy import create_engine
from openpyxl import load_workbook # write to excel
from datetime import datetime as dt
import mysql.connector
import pandas as pd
import time
import shutil # To move files
import os
import glob # For pattern matching
import time 

# Calculate excute time
start_time = time.time()
# Connecting to the server db
engine = create_engine('mysql+mysqlconnector://root:TTIdb@localhost:3306/sample', echo = False)
conn = engine.connect()
# File to insert to db(Daily sales)
df = pd.read_excel('C:/Users/jisnu/Desktop/jisnu/data/fl_sales.xlsx')
# Changing columns names 
df.columns = [c.lower() for c in df.columns]
df.columns = [c.replace(" ", "_") for c in df.columns]
df.rename(columns={'sub-producer_name':'sales_person', 'customer_name_' :'customer_name','line_group' :'line_code'}, inplace=True)
# Drop the extra data
df.drop(['agent_number','sub_producer','bind_id','product_description','column1', 'adj._code','channel_indicator'], axis =1, inplace = True)
# Splitting columns to first & last name 
df[['sales_first_name','sales_last_name']] = df['sales_person'].str.split(" ", n = 1, expand = True)
df[['customer_last_name', 'customer_first_name']] = df['customer_name'].str.split(" ", 1, True)
df.drop(['customer_name', 'sales_person'], axis =1, inplace = True)
# Reordering columns
df = df[['policy_no','sales_first_name','sales_last_name','customer_last_name','customer_first_name','line_code','product','package','item_count',
					'disposition_code','transaction_type','issued_date','date_written','written_premium']]
df.drop_duplicates(keep = 'first', inplace = True)
# Inserting to temporary table  
df.to_sql(name = 'fl_sales_temp', con = engine, if_exists = 'replace', index = False)
# Creating the temporary table
conn.execute("""
CREATE TABLE IF NOT EXISTS fl_sales_temp(
    policy_no VARCHAR(40),
    sales_first_name VARCHAR(30),
    sales_last_name VARCHAR(30),
    customer_last_name VARCHAR(50),
    customer_first_name VARCHAR(50),
    line_code VARCHAR(35),
    product VARCHAR(25),
    package VARCHAR(45),
    item_count INT,
    disposition_code VARCHAR(35),
    transaction_type VARCHAR(35),
    issued_date DATE,
    date_written DATE,
    written_premium FLOAT(8,2))
    ENGINE=INNODB;
""")
# Inserting into the table with 
conn.execute(
"""
INSERT INTO sales_new_poicy_sales_issued( 
policy_no, license, customer_last_name, customer_first_name, line_code, product, package, item_count, disposition_code,
		transaction_type,issued_date, date_written, written_premium) 
SELECT s.policy_no, e.license, s.customer_last_name, s.customer_first_name, s.line_code, s.product, s.package, s.item_count, s.disposition_code,
		s.transaction_type,s.issued_date, s.date_written, s.written_premium 
FROM fl_sales_temp AS s
LEFT JOIN employee AS e
    ON (e.first_name = s.sales_first_name AND e.last_name = s.sales_last_name)
WHERE (disposition_code = 'New Policy Issued' AND transaction_type = 'Sales Issued') AND
(
s.policy_no NOT IN (SELECT policy_no FROM sales_new_poicy_sales_issued) AND
s.issued_date NOT IN (SELECT issued_date FROM sales_new_poicy_sales_issued) AND 
s.written_premium NOT IN (SELECT written_premium FROM sales_new_poicy_sales_issued)
)
""")

conn.execute(
"""
INSERT INTO sales_new_poicy_cancel_for_cause(policy_no,license,customer_last_name, customer_first_name, line_code, product, package, item_count, disposition_code,
		transaction_type,issued_date, date_written, written_premium) 
SELECT s.policy_no, e.license, s.customer_last_name, s.customer_first_name, s.line_code, s.product, s.package, s.item_count, s.disposition_code,
        s.transaction_type,s.issued_date, s.date_written, s.written_premium 
FROM fl_sales_temp AS s
LEFT JOIN employee AS e
    ON (e.first_name = s.sales_first_name AND e.last_name = s.sales_last_name)
WHERE (disposition_code = 'New Policy Issued' AND transaction_type = 'Cancel For Cause') AND
(
s.policy_no NOT IN (SELECT policy_no FROM sales_new_poicy_cancel_for_cause) AND
s.issued_date NOT IN (SELECT issued_date FROM sales_new_poicy_cancel_for_cause) AND 
s.written_premium NOT IN (SELECT written_premium FROM sales_new_poicy_cancel_for_cause)
)
""")

conn.execute(
"""
INSERT INTO sales_new_poicy_adjustment( policy_no,license, customer_last_name, customer_first_name, line_code, product, package, item_count, disposition_code,
		transaction_type,issued_date, date_written, written_premium) 
SELECT s.policy_no, e.license, s.customer_last_name, s.customer_first_name, s.line_code, s.product, s.package, s.item_count, s.disposition_code,
        s.transaction_type,s.issued_date, s.date_written, s.written_premium 
FROM fl_sales_temp AS s
LEFT JOIN employee AS e
    ON (e.first_name = s.sales_first_name AND e.last_name = s.sales_last_name)
WHERE (disposition_code = 'New Policy Issued' AND transaction_type = 'Sales Adjustment')AND
(
s.policy_no NOT IN (SELECT policy_no FROM sales_new_poicy_adjustment) AND
s.issued_date NOT IN (SELECT issued_date FROM sales_new_poicy_adjustment) AND 
s.written_premium NOT IN (SELECT written_premium FROM sales_new_poicy_adjustment)
)
""")

conn.execute(
"""
INSERT INTO sales_add_items( policy_no,license, customer_last_name, customer_first_name, line_code, product, package, item_count, disposition_code,
		transaction_type,issued_date, date_written, written_premium) 
SELECT s.policy_no, e.license, s.customer_last_name, s.customer_first_name, s.line_code, s.product, s.package, s.item_count, s.disposition_code,
        s.transaction_type,s.issued_date, s.date_written, s.written_premium 
FROM fl_sales_temp AS s
LEFT JOIN employee AS e
    ON (e.first_name = s.sales_first_name AND e.last_name = s.sales_last_name)
WHERE disposition_code = 'Add Item' AND
(
s.policy_no NOT IN (SELECT policy_no FROM sales_add_items) AND
s.issued_date NOT IN (SELECT issued_date FROM sales_add_items) AND 
s.written_premium NOT IN (SELECT written_premium FROM sales_add_items)
)
""")

conn.execute(
"""
INSERT INTO sales_cancelled( policy_no,license, customer_last_name, customer_first_name, line_code, product, package, item_count, disposition_code,
		transaction_type,issued_date, date_written, written_premium) 
SELECT s.policy_no, e.license, s.customer_last_name, s.customer_first_name, s.line_code, s.product, s.package, s.item_count, s.disposition_code,
        s.transaction_type,s.issued_date, s.date_written, s.written_premium 
FROM fl_sales_temp AS s
LEFT JOIN employee AS e
    ON (e.first_name = s.sales_first_name AND e.last_name = s.sales_last_name)
WHERE disposition_code = 'Cancelled' AND
(
s.policy_no NOT IN (SELECT policy_no FROM sales_cancelled) AND
s.issued_date NOT IN (SELECT issued_date FROM sales_cancelled) AND 
s.written_premium NOT IN (SELECT written_premium FROM sales_cancelled)
)
""")

conn.execute(""" DROP TABLE fl_sales_temp; """)

print('Insert compelete after: {} seconds'.format(time.time() - start_time))

# string with the year+month+day for current day 
today = dt.now().date()
if today.day < 10:
  dayNo = '0'+str(today.day)
else:
  dayNo= str(today.day)
day = str(today.year)+str(today.month)+ dayNo

# Moving the file to archive after insertion with the date for the tracking
"""location_file = "C:/Users/Jisnu/Desktop/jisnu/Extra/P&C Total Serious Quotes Detail_"+day+".xlsx"
shutil.move("C:/Users/Jisnu/Desktop/jisnu/Data/P&C Total Serious Quotes Detail.xlsx", location_file)
print('Insert compelete after: {} seconds File inserted and move to Archive!!'.format(time.time() - start_time))"""

# Removes from temp file