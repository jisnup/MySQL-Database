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

# Changing the directory
os.chdir('C:/Users/Jisnu/Desktop/jisnu/data')
path_file = os.getcwd()
# filename consists double digit $ Padding zero
d = dt.now().date()
if d.day < 10: 
  dd = '0'+str(d.day)
else:
  dd = str(d.day)

if d.month < 10:
  dm = '0'+str(d.month)
else:
  dm = str(d.month)

day = str(d.year)+dm+dd

# File Name pattern
def pull_pattern(file_pattern):
	for name in glob.glob(file_pattern):
		file = os.path.join(path_file,name)
		return pd.read_csv(file)
#,skiprows = 1, header = None)

pattern = '*_CallHistory_'+day+'*.csv'
# Reading to df using function
df = pull_pattern(pattern)

df.columns = [c.lower() for c in df.columns]
df.columns = [c.replace(" ", "_") for c in df.columns]

df[['sales_last_name', 'sales_first_name']] = df['user'].str.split(", ", 1, True) # link with employee name 
df['sales_last_name'] = df['sales_last_name'].replace('mary', 'mary ann')
df[['first_name', 'last_name']] = df['lead'].str.split(" ", 1, True) # can't link, customer not in customer table yet
df['call_duration'] = df['call_duration'].map(lambda x: str(x)[:-3])
df['call_duration'] = df['call_duration'].map(lambda x: str(x).replace(":",'.'))
df.drop(['lead','user','inbound_number', 'wait_time','target_number'], axis = 1 , inplace = True)
df.rename(columns={'time':'call_date', 'group':'sales_group'}, inplace=True)
df['sales_group'].fillna(value = 'Fl', inplace=True)
df = df[df['sales_group'].str.contains("Fl")]
# Not working looking into it.  df[df['lead_id'] != '(N/A)'] 
df = df[['lead_id','sales_first_name','sales_last_name','first_name','last_name', 'sales_group', 'call_segment', 'origin', 'result','prospect_number','lead_source',
		'call_duration', 'call_date']]
df.drop_duplicates(keep = 'first', inplace = True)
df.to_sql(name = 'call_history_temp', con = engine, if_exists = 'replace', index = False)

conn.execute(
"""
CREATE TABLE IF NOT EXISTS call_history_temp(
	lead_id VARCHAR(50),
	sales_first_name VARCHAR(50),
	sales_last_name VARCHAR(50),
	first_name VARCHAR(50),
	last_name VARCHAR(50),
	sales_group VARCHAR(50),
	call_segment VARCHAR(30),
	origin VARCHAR(30),
	result VARCHAR(40),
	prospect_number VARCHAR(350),
	lead_source VARCHAR(40),
	call_duration VARCHAR(30),
	call_date VARCHAR(50))
    ENGINE=INNODB;
""")

conn.execute(
"""
INSERT IGNORE INTO call_history(
	lead_id, license,first_name,last_name,sales_group,call_segment,origin,result,prospect_number,lead_source,call_duration,call_date) 
	SELECT c.lead_id, e.license,c.first_name,c.last_name,c.sales_group,c.call_segment,c.origin,c.result,c.prospect_number,c.lead_source,c.call_duration,c.call_date 
	FROM call_history_temp AS c
	LEFT JOIN employee AS e
	ON (e.first_name = c.sales_first_name AND e.last_name = c.sales_last_name)
"""
)

conn.execute(""" TRUNCATE TABLE call_history_temp; """)
"""
Not working check into or write another script to move all the velcoify data / check if its not working because of open file.
new_location = "S:/Data Team/Archive folder/Florida Arhive file/Florida Call history Archive/"+file_name
shutil.move(file_location, new_location)
"""
print('Call History insertion compelete after: {} seconds and moved to Archive!!'.format(time.time() - start_time))

input('Press ENTER to exit')