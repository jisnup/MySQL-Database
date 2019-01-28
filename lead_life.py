"""
Author: Jisnu Prabhu
Company: Tod Todd insurance
Date: 01/11/2019
This code loads the flat file and creates and inserts the data into tempory tables. It checks if the duplicates are present.
It deletes the duplicates and inserts the rows to quotes table.
"""
import sys
sys.path.append('C://Users//Jisnu//AppData//Local//Programs//Python//Python37-32//lib//site-packages//')

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

pattern = '* Life of Lead Analysis_'+day+'*.csv'
# Reading to df using function
df = pull_pattern(pattern)

df.columns = [c.lower() for c in df.columns]
df.columns = [c.replace(" ", "_") for c in df.columns]
df[['first_name', 'last_name']] = df['log_actor'].str.split(" ", 1, True) # link with employee name 
df.drop(['log_actor'], axis = 1, inplace = True)
df = df[['first_name','last_name','log_type','id','lead_source','log_date','date_added']]
df.drop_duplicates(keep = 'first', inplace = True)

df.to_sql(name = 'lead_life_temp', con = engine, if_exists = 'replace', index = False)

conn.execute(
"""
CREATE TABLE IF NOT EXISTS lead_life_temp(
	first_name VARCHAR(30),
	last_name VARCHAR(30),
	log_type VARCHAR(20),
	id VARCHAR(30),
	lead_source VARCHAR(30),
	log_date VARCHAR(30),
	date_added VARCHAR(30))
    ENGINE=INNODB;
""")

conn.execute(
"""
INSERT IGNORE INTO lead_life(
	license, log_type, id, lead_source, log_date, date_added) 
	SELECT e.license,l.log_type, l.id, l.lead_source, l.log_date, l.date_added
	FROM lead_life_temp AS l
	LEFT JOIN employee AS e
	ON (e.first_name = l.first_name AND e.last_name = l.last_name)
"""
)

conn.execute(""" TRUNCATE TABLE lead_life_temp; """)
"""
Not working check into or write another script to move all the velcoify data / check if its not working because of open file.
new_location = "S:/Data Team/Archive folder/Florida Arhive file/Florida Call history Archive/"+file_name
shutil.move(file_location, new_location)
"""
print('Action Count insertion compelete after: {} seconds and moved to Archive!!'.format(time.time() - start_time))

input('Press ENTER to exit')

