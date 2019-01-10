from sqlalchemy import create_engine
import mysql.connector
import pandas as pd
import time

start_time = time.time()

engine = create_engine('mysql+mysqlconnector://root:TTIdb@localhost:3306/sample', echo = False)
conn = engine.connect()

df = pd.read_excel('C:/Users/jisnu/Desktop/jisnu/data/location.xlsx')
df.columns = [c.lower() for c in df.columns]
df.to_sql(name='location_temp', con= engine, if_exists = 'append', index = False)

conn.execute(
"""
CREATE TABLE IF NOT EXISTS location_temp
(
    location_id VARCHAR(20),
    location_name VARCHAR(25));
""")

conn.execute(
"""
	INSERT INTO location( location_id, location_name) 
	SELECT DISTINCT location_id, location_name FROM location_temp
	WHERE location_temp.location_id NOT IN (SELECT location_id FROM location)	
""")

conn.execute(""" DROP TABLE location_temp; """)

print('Insert compelete after: {} seconds'.format(time.time() - start_time))