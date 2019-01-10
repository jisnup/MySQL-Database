from sqlalchemy import create_engine
import mysql.connector
import pandas as pd
import time

start_time = time.time()
engine = create_engine('mysql+mysqlconnector://root:TTIdb@localhost:3306/sample', echo = False)
conn = engine.connect()

df = pd.read_excel('C:/Users/jisnu/Desktop/jisnu/data/termination.xlsx')
df.columns = [c.lower() for c in df.columns]
df.columns = [c.replace(" ", "_") for c in df.columns]
df.rename(columns={'premium\nnew':'premium_new', 'premium\nold' :'premium_old'}, inplace=True)
df.drop(['agent_number','column1','street_address','city','state','zip_code','column3','email','column2','insured_first_name','insured_last_name','phone_number',
			'line_code','anniversary_effective_date','renewal_effective_date'], axis = 1 ,inplace = True)
df.to_sql(name = 'termination_temp', con = engine, if_exists = 'append', index = False)

conn.execute(
"""
CREATE TABLE IF NOT EXISTS termination_temp(
    policy_number VARCHAR(30),
    original_year INT,
    termination_effective_date DATE,
    termination_reason VARCHAR(500),
    premium_new FLOAT(8,2),
    premium_old FLOAT(8,2),
    number_of_items INT)
    ENGINE = INNODB;
""")

conn.execute(
"""
	INSERT INTO termination(
    policy_number, original_year, termination_effective_date, termination_reason, premium_new , premium_old ,number_of_items) 
	SELECT DISTINCT policy_number, original_year, termination_effective_date, termination_reason, premium_new , premium_old ,number_of_items FROM termination_temp 
	WHERE termination_temp.policy_number NOT IN (SELECT policy_number FROM termination)	
""")

conn.execute(""" DROP TABLE termination_temp; """)

print('Insert compelete after: {} seconds'.format(time.time() - start_time))

# Done