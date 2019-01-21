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

pattern = '* Action Count_'+day+'*.csv'
# Reading to df using function
df = pull_pattern(pattern)

df.columns = [c.lower() for c in df.columns]
df.columns = [c.replace(" ", "_") for c in df.columns]
df.rename(columns={'count(log_type)':'total_action'}, inplace=True)
df[['first_name', 'last_name']] = df['log_actor'].str.split(" ", 1, True) # link with employee name 
df['action_date'] = dt.now().date()
df.drop(['log_actor'], axis = 1, inplace = True)
df = df[['first_name','last_name','total_action','action_date']]
df.drop_duplicates(keep = 'first', inplace = True)

df.to_sql(name = 'action_count_temp', con = engine, if_exists = 'append', index = False)

conn.execute(
"""
CREATE TABLE IF NOT EXISTS action_count_temp(
	first_name VARCHAR(30),
	last_name VARCHAR(30),
	total_action VARCHAR(30),
	action_date VARCHAR(30))
    ENGINE=INNODB;
""")

conn.execute(
"""
INSERT IGNORE INTO action_count(
	license, total_action, action_date) 
	SELECT e.license,c.total_action, c.action_date 
	FROM action_count_temp AS c
	LEFT JOIN employee AS e
	ON (e.first_name = c.first_name AND e.last_name = c.last_name)
"""
)

conn.execute(""" TRUNCATE TABLE action_count_temp; """)
"""
Not working check into or write another script to move all the velcoify data / check if its not working because of open file.
new_location = "S:/Data Team/Archive folder/Florida Arhive file/Florida Call history Archive/"+file_name
shutil.move(file_location, new_location)
"""
print('Action Count insertion compelete after: {} seconds and moved to Archive!!'.format(time.time() - start_time))

input('Press ENTER to exit')

