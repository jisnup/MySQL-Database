from sqlalchemy import create_engine
import mysql.connector
import pandas as pd
start_time = time.time()

engine = create_engine('mysql+mysqlconnector://root:TTIdb@localhost:3306/sample', echo = False)
conn = engine.connect()

df = pd.read_excel('C:/Users/jisnu/Desktop/jisnu/data/business.xlsx')
df.columns = [c.lower() for c in df.columns]
df.to_sql(name='business_temp', con= engine, if_exists = 'append', index = False)

conn.execute(
"""
CREATE TABLE IF NOT EXISTS business_temp
(
    business_unit VARCHAR(10),
    business_location VARCHAR(15));
""")

conn.execute(
"""
	INSERT INTO business( business_unit, business_location) 
	SELECT DISTINCT business_unit, business_location FROM business_temp
	WHERE business_temp.business_unit NOT IN (SELECT business_unit FROM business)	
""")

conn.execute(""" DROP TABLE business_temp; """)

print('Insert compelete after: {} seconds'.format(time.time() - start_time))

#line code same number for differ