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
	pattern = '* Past Due X-Date Analysis_'+day+'*.csv'
	# Reading to df using function
	df = pull_pattern(pattern)

	df.columns = [c.lower() for c in df.columns]
	df.columns = [c.replace(" ", "_") for c in df.columns]
	df.columns = [c.replace("-", "_") for c in df.columns]
	df[['last_name', 'first_name']] = df['user'].str.split(",", 1, True) # link with employee name
	df.drop(['user'], axis = 1, inplace = True)

	booleanDictionary = {True: 'TRUE', False: 'FALSE'}
	df = df.replace(booleanDictionary)

	df['x_date'] = pd.to_datetime(df['x_date']).apply(lambda x: x.date()) 
	df = df[['first_name','last_name','id','lead_source','lead_score','date_added','last_action_date','status', 'x_date','flagged']]

	df.drop_duplicates(keep = 'first', inplace = True)

	df.to_sql(name = 'past_x_dates_temp', con = engine, if_exists = 'replace', index = False)

	conn.execute(
	"""
	CREATE TABLE IF NOT EXISTS past_x_dates_temp(
		first_name VARCHAR(30),
		last_name VARCHAR(30),
		id VARCHAR(30),
		lead_source VARCHAR(30),
		lead_score VARCHAR(30),
		date_added VARCHAR(30),
		last_action_date VARCHAR(30),
		status VARCHAR(30),
		x_date VARCHAR(30),
		flagged VARCHAR(30))
	    ENGINE=INNODB;
	""")

	conn.execute(
	"""
	INSERT IGNORE INTO past_x_dates(
		license, id, lead_source, lead_score, date_added, last_action_date, status, x_date, flagged) 
		SELECT e.license,p.id, p.lead_source, p.lead_score, p.date_added, p.last_action_date, p.status, p.x_date, p.flagged
		FROM past_x_dates_temp AS p
		LEFT JOIN employee AS e
		ON (e.first_name = p.first_name AND e.last_name = p.last_name)
	""")

	conn.execute(""" TRUNCATE TABLE past_x_dates_temp; """)
	file_name = pull_file_name(pattern)
	new_location = "S:/Data Team/Archive folder/Florida Archive file/Florida Past Due X-Dates Archive/"+file_name
	file_location = pull_file_path(pattern)
	shutil.move(file_location, new_location)

	print('\n Action Count insertion compelete after: {} seconds and moved to Archive!!'.format(time.time() - start_time))

except:
	print('\n ERROR !!  ** File not uploaded ** (Past X-Dates)')


try:
	pattern = '* Past Due Follow-Up Analysis_'+day+'*.csv'
	# Reading to df using function
	df = pull_pattern(pattern)

	df.columns = [c.lower() for c in df.columns]
	df.columns = [c.replace(" ", "_") for c in df.columns]
	df.columns = [c.replace("-", "_") for c in df.columns]

	df[['last_name', 'first_name']] = df['user'].str.split(", ", 1, True) # link with employee name
	df.drop(['user'], axis = 1, inplace = True)

	booleanDictionary = {True: 'TRUE', False: 'FALSE'}
	df = df.replace(booleanDictionary)
	df['follow_up_date'] = pd.to_datetime(df['follow_up_date']).apply(lambda x: x.date()) 
	df = df[['first_name','last_name','id','lead_source','lead_score','date_added','last_action_date','status', 'follow_up_date','flagged']]

	df.drop_duplicates(keep = 'first', inplace = True)

	df.to_sql(name = 'past_follow_ups_temp', con = engine, if_exists = 'replace', index = False)

	conn.execute(
	"""
	CREATE TABLE IF NOT EXISTS past_follow_ups_temp(
		first_name VARCHAR(30),
		last_name VARCHAR(30),
		id VARCHAR(30),
		lead_source VARCHAR(30),
		lead_score VARCHAR(30),
		date_added VARCHAR(30),
		last_action_date VARCHAR(30),
		status VARCHAR(30),
		follow_up_date VARCHAR(30),
		flagged VARCHAR(30))
	    ENGINE=INNODB;
	""")

	conn.execute(
	"""
	INSERT IGNORE INTO past_follow_ups(
		license, id, lead_source, lead_score, date_added, last_action_date, status, follow_up_date, flagged) 
		SELECT e.license,p.id, p.lead_source, p.lead_score, p.date_added, p.last_action_date, p.status, p.follow_up_date, p.flagged
		FROM past_follow_ups_temp AS p
		LEFT JOIN employee AS e
		ON (e.first_name = p.first_name AND e.last_name = p.last_name)
	""")

	conn.execute(""" TRUNCATE TABLE past_follow_ups_temp; """)
	file_name = pull_file_name(pattern)
	new_location = "S:/Data Team/Archive folder/Florida Archive file/Florida Past Due Follow Ups Archive/"+file_name
	file_location = pull_file_path(pattern)
	shutil.move(file_location, new_location)

	print('\n Past follow ups insertion compelete after: {} seconds and moved to Archive!!'.format(time.time() - start_time))

except:
	print('\n ERROR !!  ** File not uploaded ** (Follow up file missing)')

input('\n Enter: ')