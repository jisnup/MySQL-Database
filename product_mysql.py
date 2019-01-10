from sqlalchemy import create_engine
import mysql.connector
import pandas as pd
start_time = time.time()

engine = create_engine('mysql+mysqlconnector://root:TTIdb@localhost:3306/sample', echo = False)
conn = engine.connect()

df = pd.read_excel('C:/Users/jisnu/Desktop/jisnu/data/product.xlsx')
df.columns = [c.lower() for c in df.columns]
df.to_sql(name='product_temp', con= engine, if_exists = 'append', index = False)

conn.execute(
"""
CREATE TABLE IF NOT EXISTS product_temp
(
    line_code VARCHAR(20),
    product VARCHAR(50));
""")

conn.execute(
"""
	INSERT INTO product( line_code, product) 
	SELECT DISTINCT line_code, product FROM product_temp
	WHERE product_temp.line_code NOT IN (SELECT line_code FROM product)	
""")

conn.execute(""" DROP TABLE product_temp; """)

print('Insert compelete after: {} seconds'.format(time.time() - start_time))