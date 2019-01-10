from sqlalchemy import create_engine
import mysql.connector
import pandas as pd
import time

start_time = time.time()
engine = create_engine('mysql+mysqlconnector://root:TTIdb@localhost:3306/sample', echo = False)
conn = engine.connect()

df = pd.read_excel('C:/Users/jisnu/Desktop/jisnu/data/fl_sales.xlsx')

df.columns = [c.lower() for c in df.columns]
df.columns = [c.replace(" ", "_") for c in df.columns]
df.drop(['agent_number','sub_producer','bind_id','product_description','column1', 'adj._code','channel_indicator'], axis =1, inplace = True)
df.rename(columns={'sub-producer_name':'sales_person', 'customer_name_' :'customer_name','line_group' :'line_code'}, inplace=True)

df[['sales_first_name','sales_last_name']] = df['sales_person'].str.split(" ", n = 1, expand = True)
df[['customer_last_name', 'customer_first_name']] = df['customer_name'].str.split(" ", 1, True)

df.drop(['customer_name', 'sales_person'], axis =1, inplace = True)
df = df[['policy_no','sales_first_name','sales_last_name','customer_last_name','customer_first_name','line_code','product','package','item_count',
					'disposition_code','transaction_type','issued_date','date_written','written_premium']]
df.to_sql(name = 'fl_sales_temp', con = engine, if_exists = 'append', index = False)


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

conn.execute(
"""
INSERT INTO sales_new_poicy_sales_issued( policy_no,sales_first_name,sales_last_name, customer_last_name, customer_first_name, line_code, product, package, item_count, disposition_code,
		transaction_type,issued_date, date_written, written_premium) 
SELECT policy_no, sales_first_name,sales_last_name, customer_last_name, customer_first_name, line_code, product, package, item_count, disposition_code,
		transaction_type,issued_date, date_written, written_premium 
FROM fl_sales_temp
WHERE (disposition_code = 'New Policy Issued' AND transaction_type = 'Sales Issued')
""")

conn.execute(
"""
INSERT INTO sales_new_poicy_cancel_for_cause( policy_no,sales_first_name,sales_last_name, customer_last_name, customer_first_name, line_code, product, package, item_count, disposition_code,
		transaction_type,issued_date, date_written, written_premium) 
SELECT policy_no, sales_first_name,sales_last_name, customer_last_name, customer_first_name, line_code, product, package, item_count, disposition_code,
		transaction_type,issued_date, date_written, written_premium 
FROM fl_sales_temp
WHERE (disposition_code = 'New Policy Issued' AND transaction_type = 'Cancel For Cause')
""")

conn.execute(
"""
INSERT INTO sales_new_poicy_adjustment( policy_no,sales_first_name,sales_last_name, customer_last_name, customer_first_name, line_code, product, package, item_count, disposition_code,
		transaction_type,issued_date, date_written, written_premium) 
SELECT policy_no, sales_first_name,sales_last_name, customer_last_name, customer_first_name, line_code, product, package, item_count, disposition_code,
		transaction_type,issued_date, date_written, written_premium 
FROM fl_sales_temp
WHERE (disposition_code = 'New Policy Issued' AND transaction_type = 'Sales Adjustment')
""")

conn.execute(
"""
INSERT INTO sales_add_items( policy_no,sales_first_name,sales_last_name, customer_last_name, customer_first_name, line_code, product, package, item_count, disposition_code,
		transaction_type,issued_date, date_written, written_premium) 
SELECT policy_no, sales_first_name,sales_last_name, customer_last_name, customer_first_name, line_code, product, package, item_count, disposition_code,
		transaction_type,issued_date, date_written, written_premium 
FROM fl_sales_temp
WHERE disposition_code = 'Add Item'
""")

conn.execute(
"""
INSERT INTO sales_cancelled( policy_no,sales_first_name,sales_last_name, customer_last_name, customer_first_name, line_code, product, package, item_count, disposition_code,
		transaction_type,issued_date, date_written, written_premium) 
SELECT policy_no, sales_first_name,sales_last_name, customer_last_name, customer_first_name, line_code, product, package, item_count, disposition_code,
		transaction_type,issued_date, date_written, written_premium 
FROM fl_sales_temp
WHERE disposition_code = 'Cancelled'
""")

conn.execute(""" DROP TABLE fl_sales_temp; """)

print('Insert compelete after: {} seconds'.format(time.time() - start_time))


# Deal with duplicates
# join sales person name with id