from sqlalchemy import create_engine
import mysql.connector
import pandas as pd
import time

start_time = time.time()

engine = create_engine('mysql+mysqlconnector://root:TTIdb@localhost:3306/sample', echo = False)
conn = engine.connect()

df = pd.read_excel('C:/Users/jisnu/Desktop/jisnu/data/customer.xlsx')
df.columns = [c.lower() for c in df.columns]

df.to_sql(name='customer_temp', con= engine, if_exists = 'append', index = False)

conn.execute(
"""
CREATE TABLE IF NOT EXISTS customer_temp
(
    first_name VARCHAR(20),
    last_name VARCHAR(20),
    dob VARCHAR(20),
    gender CHAR(6),
    phone VARCHAR(300),
    email_id VARCHAR(320),
    marital_status VARCHAR(20),
    address VARCHAR(300),
    city VARCHAR(50),
    state VARCHAR(25),
    zipcode VARCHAR(10),
    persona VARCHAR(10),
    easy_pay VARCHAR(15),
    e_policy VARCHAR(15),
    e_bill VARCHAR(15),
    original_year INT,
    client_status VARCHAR(15));
""")

conn.execute(
"""
    INSERT INTO customer(
    first_name,last_name,dob,gender,phone,email_id,marital_status,address,city,state,zipcode,persona,easy_pay,e_policy,e_bill,original_year,client_status) 
    SELECT     
    first_name,last_name,dob,gender, phone,email_id,marital_status,address,city,state,zipcode,persona,easy_pay,e_policy,e_bill,original_year,client_status 
    FROM customer_temp
""")

conn.execute(
"""
    DELETE a
    FROM customer AS a
    LEFT JOIN(
    SELECT max(customer_id),first_name, last_name, phone, email_id
    FROM customer
    GROUP BY first_name, last_name, phone, email_id) AS b
    ON a.customer_id = b.customer_id and a.first_name = b.first_name and a.last_name = b.last_name and a.phone = b.phone and a.email_id = b.email_id
    where b.customer_id IS NULL;
""")

conn.execute(""" DROP TABLE customer_temp; """)

print('Insert compelete after: {} seconds'.format(time.time() - start_time))