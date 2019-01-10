from sqlalchemy import create_engine
import mysql.connector
import pandas as pd
start_time = time.time()

engine = create_engine('mysql+mysqlconnector://root:TTIdb@localhost:3306/sample', echo = False)
conn = engine.connect()

df = pd.read_excel('C:/Users/jisnu/Desktop/jisnu/data/department.xlsx')
df.columns = [c.lower() for c in df.columns]
df.to_sql(name='department_temp', con= engine, if_exists = 'append', index = False)

conn.execute(
"""
CREATE TABLE IF NOT EXISTS department_temp
(
    department_id INT,
    department_location VARCHAR(25));
""")

conn.execute(
"""
	INSERT INTO department( department_id, department_location) 
	SELECT DISTINCT department_id, department_location FROM department_temp
	WHERE department_temp.department_id NOT IN (SELECT department_id FROM department)	
""")

conn.execute(""" DROP TABLE department_temp; """)

print('Insert compelete after: {} seconds'.format(time.time() - start_time))