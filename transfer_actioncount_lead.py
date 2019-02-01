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
day = dt.now().strftime("%Y%m%d")

# File Name pattern
def pull_pattern(file_pattern):
	for name in glob.glob(file_pattern):
		file = os.path.join(path_file,name)
		return pd.read_csv(file)

def pull_file_name(file_pattern):
	for name in glob.glob(file_pattern):
		return name

def pull_file_path(file_pattern):
	for name in glob.glob(file_pattern):
		file = os.path.join(path_file,name)
		return file

try:
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
	""")

	conn.execute(""" TRUNCATE TABLE lead_life_temp; """)
	"""
	Not working check into or write another script to move all the velcoify data / check if its not working because of open file.
	new_location = "S:/Data Team/Archive folder/Florida Archive file/Florida Call history Archive/"+file_name
	shutil.move(file_location, new_location)
	"""
	print('\n Lead Life insertion compelete after: {} seconds and moved to Archive!!'.format(time.time() - start_time))

except:
	print("\n ERROR File not uploaded (Lead Life)")

try:
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
	print('\n Action Count insertion compelete after: {} seconds and moved to Archive!!'.format(time.time() - start_time))

except:
	print('\n ERROR File not uploaded (Action Count)')

try:
	pattern = '* Transfer Report (KPI)_'+day+'*.csv'
	# Reading to df using function
	df = pull_pattern(pattern)

	df.columns = [c.lower() for c in df.columns]
	df.columns = [c.replace(" ", "_") for c in df.columns]
	df.transfer_date = pd.to_datetime(df['transfer_date']).apply(lambda x: x.date()) 
	#df[['first_caller','last_caller']] = df['caller'].str.split(" ", 1, True) # link with employee name
	df[['sales_first_name','sales_last_name']] = df['transferred_to'].str.split(" ", 1, True) # link with employee name
	df.drop(['transferred_to'], axis = 1, inplace = True)
	df = df[['caller','sales_first_name','sales_last_name', 'transfer_date','date_added','transfer_type','id','milestone','lead_source',
				'status','1st_vehicle_make','2nd_vehicle_make','3rd_vehicle_make','4th_vehicle_make']]

	df.drop_duplicates(keep = 'first', inplace = True)

	df.to_sql(name = 'transfer_temp', con = engine, if_exists = 'replace', index = False)

	conn.execute(
	"""
	CREATE TABLE IF NOT EXISTS transfer_temp(
		caller VARCHAR(30),
		sales_first_name VARCHAR(30),
		sales_last_name VARCHAR(30),
		transfer_date VARCHAR(30),
		date_added VARCHAR(30),
		transfer_type VARCHAR(30),
		id VARCHAR(30),
		milestone VARCHAR(30),
		lead_source VARCHAR(30),
		status VARCHAR(30),
		1st_vehicle_make VARCHAR(30),
		2nd_vehicle_make VARCHAR(30),
		3rd_vehicle_make VARCHAR(30),
		4th_vehicle_make VARCHAR(30))
	    ENGINE=INNODB;
	""")

	conn.execute(
	"""
	INSERT IGNORE INTO transfer(
		caller, license, transfer_date,date_added,transfer_type,id,milestone,lead_source,status,1st_vehicle_make,2nd_vehicle_make,3rd_vehicle_make,4th_vehicle_make) 
		SELECT t.caller, e.license, t.transfer_date,t.date_added,t.transfer_type,t.id,t.milestone,t.lead_source,t.status,t.1st_vehicle_make,t.2nd_vehicle_make,t.3rd_vehicle_make,t.4th_vehicle_make
		FROM transfer_temp AS t
		LEFT JOIN employee AS e
		ON (e.first_name = t.sales_first_name AND e.last_name = t.sales_last_name)
	""")

	conn.execute(""" TRUNCATE TABLE transfer_temp; """)
	file_name = pull_file_name(pattern)
	new_location = "S:/Data Team/Archive folder/Florida Archive file/Florida Transfers Archive/"+file_name
	file_location = pull_file_path(pattern)
	shutil.move(file_location, new_location)

	print('\n Transfers insertion compelete after: {} seconds and moved to Archive!!'.format(time.time() - start_time))

except:
	print('\n ERROR File not uploaded (Transfers) ')

input('\n Press ENTER to exit')