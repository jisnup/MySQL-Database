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
from datetime import date, timedelta
import os
import glob # For pattern matching
import time 
import shutil # To move files

#Connecting to the server db
engine = create_engine('mysql+mysqlconnector://****:****@######:#####/georgia', echo = False)
conn = engine.connect()

os.chdir('S:/Data Team/DB Raw files/georgia')
path_file = os.getcwd()
# filename consists double digit $ Padding zero

#yesterday = date.today() - timedelta(4)
#day = yesterday.strftime("%Y%m%d")
day = dt.now().strftime("%Y%m%d")

yesterday = date.today() - timedelta(1)

# File Name pattern
def pull_pattern(file_pattern):
	""" searchs for the file pattern in the file path and returns the read command for the file."""
	for name in glob.glob(file_pattern):
		file = os.path.join(path_file,name)
		return pd.read_csv(file)

def pull_file_name(file_pattern):
	""" Returns the file name (used to move file to different location)"""
	for name in glob.glob(file_pattern):
		return name
		
def pull_file_path(file_pattern):
	""" Returns the file path (used to identify the location of the file to move)"""
	for name in glob.glob(file_pattern):
		file = os.path.join(path_file,name)
		return file

print('\n Georgia Daily Data Upload - '+ day)
"""------------------------------------------------------------------------------------------------------------------------------------------
									 					Georgia Quotes 					             									  """

try:
	# Calculate excute time
	start_time = time.time()
	# File to insert to db(Daily Quotes)
	file_name = 'S:/Data Team/DB Raw files/georgia/P&C Total Serious Quotes Detail.xlsx'
	#file_name = 'S:/Data Team/DB Raw files/georgia/fl_quote2.xlsx'
	df = pd.read_excel(file_name,skiprows = 10)
	#df = pd.read_excel(file_name, nrows = 30500)

	df.drop(['Unnamed: 0','Unnamed: 12'],axis = 1 , inplace = True)
	df.drop(df.index[len(df) - 1], axis = 0, inplace = True)
	# Changing columns names 
	df.columns = [c.lower() for c in df.columns]
	df.columns = [c.replace(" ", "_") for c in df.columns]
	df.rename(columns={'quote_control_number' : 'quote_number'}, inplace=True)
	# Adding zero's to subproducers code (empolyee table has no, code)
	df.sub_producer = df.sub_producer.replace('No Code','000')
	df['sub_producer'] = df['sub_producer'].str[:3] # Sub code gets recycled 
	df[['customer_last_name', 'customer_first_name']] = df['customer_name'].str.split(",", 1, True) # can't link, customer not in customer table yet
	df.drop(['customer_name','customer_state','customer_zip_code','channel'], axis = 1 , inplace = True)
	df = df[['agent_number', 'quote_number', 'sub_producer','customer_first_name','customer_last_name', 'production_date', 'product', 'quoted_item_count', 'quoted_premium']]
	df.drop_duplicates(keep = 'first', inplace = True)
	# Loads data into a tempory file.
	df.to_sql(name = 'ga_quotes_temp', con = engine, if_exists = 'replace', index = False)

	conn.execute(
	"""
	CREATE TABLE IF NOT EXISTS ga_quotes_temp(
		agent_number VARCHAR(30),
		quote_number VARCHAR(50),
		sub_producer INT,
		customer_first_name VARCHAR(50),
		customer_last_name VARCHAR(50),
		production_date VARCHAR(30),
		product VARCHAR(50),
		quoted_item_count INT,
		quoted_premium FLOAT(8,2))
	    ENGINE=INNODB;
	""")
	# If it becomes slow later select the quotes number from last last week

	conn.execute(
	"""
	INSERT IGNORE INTO ga_quotes(
		agent_number, quote_number, sub_producer, customer_first_name, customer_last_name, production_date, product ,quoted_item_count ,quoted_premium) 
		SELECT agent_number, quote_number, sub_producer, customer_first_name, customer_last_name, production_date, product , quoted_item_count , quoted_premium 
		FROM ga_quotes_temp
		WHERE (quote_number NOT IN (SELECT quote_number FROM ga_quotes) AND production_date NOT IN (SELECT production_date FROM ga_quotes))
		ORDER BY production_date 
	""")

	conn.execute(""" DROP TABLE ga_quotes_temp; """)


	# Moving the file to archive after insertion with the date for the tracking
	location_file = "S:/Data Team/Archive folder/Georgia Archive file/Georgia Quotes Archive/P&C Total Serious Quotes Detail_"+day+".xlsx"
	shutil.move("S:/Data Team/DB Raw files/georgia/P&C Total Serious Quotes Detail.xlsx", location_file)
	print('\n 1. "Georgia quotes" insert compelete after: {} seconds File inserted and move to Archive!!'.format(time.time() - start_time))

except:
	print('\n 1. ERROR !!  ** File not uploaded ** (Georgia quotes)')

"""------------------------------------------------------------------------------------------------------------------------------------------
									 					FL New Business  Detailed Report 												  """
try:
	# Calculate excute time
	start_time = time.time()
	# File to insert to db(Daily sales)
	#df = pd.read_excel('S:/Data Team/DB Raw files/georgia/ga_sales.xlsx')
	file_name = 'S:/Data Team/DB Raw files/georgia/P&C New Business Production Detailed Report.xlsx'
	df = pd.read_excel(file_name, skiprows = 9, header = None)
	# File comes from Allstate (bad formatting)
	df.drop([0,17,19],axis = 1 , inplace = True)
	# last 9 rows needs to be dropped
	df.drop(df.index[-9:len(df)], axis = 0, inplace = True)
	# Few rows needs to be dropped, its changes every today so find the number of unique rows and add 2 empty rows and drops them.
	unique_rows = df[1].nunique()
	df.drop(df.index[0:unique_rows+2], axis = 0, inplace = True)
	new_header = df.iloc[0] #grab the first row for the header
	df = df[1:] #take the data less the header row
	df.columns = new_header #set the header row as the df header"""
	# Changing columns names 
	df.columns = [c.lower() for c in df.columns]
	df.columns = [c.replace(" ", "_") for c in df.columns]
	df.rename(columns={'sub-producer_name':'sales_person', 'customer_name_' :'customer_name','line_group' :'line_code'}, inplace=True)
	# Drop the extra data
	df.drop(['sales_person','bind_id','channel_indicator'], axis =1, inplace = True)
	# 'column1', 'adj._code',
	# Splitting columns to first & last name 
	df[['customer_last_name', 'customer_first_name']] = df['customer_name'].str.split(" ", 1, True)
	df.drop(['customer_name'], axis =1, inplace = True)
	# Reordering columns
	df = df[['agent_number','policy_no','sub_producer','customer_last_name','customer_first_name','line_code','product','product_description','package','item_count',
					'disposition_code','transaction_type','issued_date','date_written','written_premium']]
	# Short date
	df['issued_date'] = df['issued_date'].map(lambda x: str(x)[:10])
	df['date_written'] = df['date_written'].map(lambda x: str(x)[:10])
	df.drop_duplicates(keep = 'first', inplace = True) # Keeps first duplicates  
	df.to_sql(name = 'ga_sales_temp', con = engine, if_exists = 'replace', index = False)  # Inserting to temporary table 
	# Creating the temporary table
	conn.execute(
	"""
	CREATE TABLE IF NOT EXISTS ga_sales_temp(
		agent_number VARCHAR(30),
	    policy_no VARCHAR(40),
	    sub_producer VARCHAR(30),
	    customer_last_name VARCHAR(50),
	    customer_first_name VARCHAR(50),
	    line_code VARCHAR(35),
	    product VARCHAR(25),
	    package VARCHAR(45),
	    product_description VARCHAR(50),
	    item_count INT,
	    disposition_code VARCHAR(35),
	    transaction_type VARCHAR(35),
	    issued_date DATE,
	    date_written DATE,
	    written_premium FLOAT(8,2))
	    ENGINE=INNODB;
	""")
	# Inserting into the table with 
	conn.execute(
	"""
	INSERT IGNORE INTO ga_sales( agent_number, policy_no, sub_producer, customer_last_name, customer_first_name, line_code, product, package, product_description, item_count, disposition_code,
	        transaction_type, issued_date, date_written, written_premium) 
	SELECT agent_number, policy_no, sub_producer, customer_last_name, customer_first_name, line_code, product, package, product_description, item_count, disposition_code,
	        transaction_type, issued_date, date_written, written_premium
	FROM ga_sales_temp
	""")

	conn.execute(""" DROP TABLE ga_sales_temp; """)
	# Moving the file to archive after insertion with the date for the tracking
	location_file = "S:/Data Team/Archive folder/Georgia Archive file/Georgia Sales Archive/P&C New Business Production Detailed Report_"+day+".xlsx"
	shutil.move("S:/Data Team/DB Raw files/georgia/P&C New Business Production Detailed Report.xlsx", location_file)
	print('\n 2. "Georgia Sales" Insert compelete after: {} seconds File inserted and move to Archive!!'.format(time.time() - start_time))

except:
	print('\n 2. ERROR !!  ** File not uploaded ** (Georgia sales)')

"""------------------------------------------------------------------------------------------------------------------------------------------
									 				Termination Report 	                    											  """
try:
	# Calculate excute time
	start_time = time.time()

	#df = pd.read_excel('S:/Data Team/DB Raw files/georgia/termination.xlsx')
	file_name = 'S:/Data Team/DB Raw files/georgia/Termination Audit Report Date Range.xlsx'
	df = pd.read_excel(file_name, skiprows = 8)
	df = df[df.columns.drop(list(df.filter(regex='unnamed:_')))]

	# Changing columns names 
	df.columns = [c.lower() for c in df.columns]
	df.columns = [c.replace(" ", "_") for c in df.columns]
	# Dropping extra data
	df.drop(['street_address','city','state','zip_code','email', 'insured_first_name','insured_last_name','phone_number',
            'line_code','anniversary_effective_date','renewal_effective_date'], axis = 1 ,inplace = True)

	# Changing columns names
	df.rename(columns={'premium\nnew':'premium_new', 'premium\nold' :'premium_old'}, inplace=True)
	df.drop_duplicates(keep = 'first', inplace = True)

	# Inserting into the table with 
	df.to_sql(name = 'termination_temp', con = engine, if_exists = 'replace', index = False)

	conn.execute(
	"""
	CREATE TABLE IF NOT EXISTS termination_temp(
		agent_number VARCHAR(30),
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
		INSERT IGNORE INTO termination(agent_number, policy_number, original_year, termination_effective_date, termination_reason, premium_new , premium_old ,number_of_items) 
	    SELECT agent_number, policy_number, original_year, termination_effective_date, termination_reason, premium_new , premium_old ,number_of_items FROM termination_temp 
	    WHERE termination_temp.policy_number NOT IN (SELECT policy_number FROM termination) 

	""")

	conn.execute(""" DROP TABLE termination_temp; """)
	# Moving the file to archive after insertion with the date for the tracking
	
	location_file = "S:/Data Team/Archive folder/Georgia Archive file/Georgia Termination Archive/Termination Audit Report Date Range_"+day+".xlsx"
	shutil.move("S:/Data Team/DB Raw files/georgia/Termination Audit Report Date Range.xlsx", location_file)
	print('\n 3. "Termination" report Insert compelete after: {} seconds and moved to Archive!!'.format(time.time() - start_time))

except:
	print('\n 3. ERROR !!  ** File not uploaded ** (Termination)')

"""------------------------------------------------------------------------------------------------------------------------------------------
									 					Past Due X-Date Analysis		             								       """
 
try:
	# Calculate excute time
	start_time = time.time()
	pattern = '* Past Due X-Date Analysis (GA)_'+day+'*.csv'
	# Reading to df using function
	df = pull_pattern(pattern)

	df.columns = [c.lower() for c in df.columns]
	df.columns = [c.replace(" ", "_") for c in df.columns]
	df.columns = [c.replace("-", "_") for c in df.columns]
	df[['last_name', 'first_name']] = df['user'].str.split(", ", 1, True) # link with employee name
	df.drop(['user'], axis = 1, inplace = True)

	booleanDictionary = {True: 'TRUE', False: 'FALSE'}
	df = df.replace(booleanDictionary)
 
	df['x_date'] = pd.to_datetime(df['x_date']).apply(lambda x: x.date()) 
	df = df[['first_name','last_name','id','lead_source','lead_score','date_added','last_action_date','status', 'x_date','flagged']]
	df = df[df['x_date'] == yesterday]
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
		sub_producer, id, lead_source, lead_score, date_added, last_action_date, status, x_date, flagged) 
		SELECT e.sub_producer, p.id, p.lead_source, p.lead_score, p.date_added, p.last_action_date, p.status, p.x_date, p.flagged
		FROM past_x_dates_temp AS p
		LEFT JOIN employee AS e
		ON (e.first_name = p.first_name AND e.last_name = p.last_name)
	""")

	conn.execute(""" DROP TABLE past_x_dates_temp; """)
	file_name = pull_file_name(pattern)
	new_location = "S:/Data Team/Archive folder/Georgia Archive file/Georgia Past Due X-Dates Archive/"+file_name
	file_location = pull_file_path(pattern)
	shutil.move(file_location, new_location)

	print('\n 4. "Past Due X-Date" Analysis insertion compelete after: {} seconds and moved to Archive!!'.format(time.time() - start_time))

except:
	print('\n 4. ERROR !!  ** File not uploaded ** (Past X-Dates)')

"""------------------------------------------------------------------------------------------------------------------------------------------
									 				  Past Due Follow-Up Analysis		             								       """
 
try:
	# Calculate excute time
	start_time = time.time()

	pattern = '* Past Due Follow-Up Analysis (GA)_'+day+'*.csv'
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
	df = df[df['follow_up_date'] == yesterday]
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
		sub_producer, id, lead_source, lead_score, date_added, last_action_date, status, follow_up_date, flagged) 
		SELECT e.sub_producer, p.id, p.lead_source, p.lead_score, p.date_added, p.last_action_date, p.status, p.follow_up_date, p.flagged
		FROM past_follow_ups_temp AS p
		LEFT JOIN employee AS e
		ON (e.first_name = p.first_name AND e.last_name = p.last_name)
	""")

	conn.execute(""" DROP TABLE past_follow_ups_temp; """)

	file_name = pull_file_name(pattern)
	new_location = "S:/Data Team/Archive folder/Georgia Archive file/Georgia Past Due Follow Ups Archive/"+file_name
	file_location = pull_file_path(pattern)
	shutil.move(file_location, new_location)

	print('\n 5. "Past follow ups" insertion compelete after: {} seconds and moved to Archive!!'.format(time.time() - start_time))

except:
	print('\n 5. ERROR !!  ** File not uploaded ** (Follow up file missing)')

"""------------------------------------------------------------------------------------------------------------------------------------------
									 				  		Life of Lead Analysis	                 								       """

try:
	# Calculate excute time
	start_time = time.time()
	pattern = '* Life of Lead Analysis (GA)_'+day+'*.csv'
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
		sub_producer, log_type, id, lead_source, log_date, date_added) 
		SELECT e.sub_producer, l.log_type, l.id, l.lead_source, l.log_date, l.date_added
		FROM lead_life_temp AS l
		LEFT JOIN employee AS e
		ON (e.first_name = l.first_name AND e.last_name = l.last_name)
	""")

	conn.execute(""" DROP TABLE lead_life_temp; """)
	file_name = pull_file_name(pattern)
	new_location = "S:/Data Team/Archive folder/Georgia Archive file/Georgia Life of Leads Archive/"+file_name
	file_location = pull_file_path(pattern)
	shutil.move(file_location, new_location)
	print('\n 6. "Lead Life" insertion compelete after: {} seconds and moved to Archive!!'.format(time.time() - start_time))

except:
	print("\n 6. ERROR !!  ** File not uploaded ** (Lead Life)")

"""------------------------------------------------------------------------------------------------------------------------------------------
									 				  			Action Count Table	                 								       """
try:
	# Calculate excute time
	start_time = time.time()

	pattern = '*Action Count (GA)_'+day+'*.csv'
	# Reading to df using function
	df = pull_pattern(pattern)

	df.columns = [c.lower() for c in df.columns]
	df.columns = [c.replace(" ", "_") for c in df.columns]
	df.rename(columns={'count(log_type)':'total_action'}, inplace=True)
	df[['first_name', 'last_name']] = df['log_actor'].str.split(" ", 1, True) # link with employee name 
	df['action_date'] = yesterday
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
		sub_producer, total_action, action_date) 
		SELECT e.sub_producer, c.total_action, c.action_date 
		FROM action_count_temp AS c
		LEFT JOIN employee AS e
		ON (e.first_name = c.first_name AND e.last_name = c.last_name)
	"""
	)

	conn.execute(""" DROP TABLE action_count_temp; """)
	file_name = pull_file_name(pattern)
	new_location = "S:/Data Team/Archive folder/Georgia Archive file/Georgia Action Count Archive/"+file_name
	file_location = pull_file_path(pattern)
	shutil.move(file_location, new_location)
	print('\n 7. "Action Count" insertion compelete after: {} seconds and moved to Archive!!'.format(time.time() - start_time))

except:
	print('\n 7. ERROR !!  ** File not uploaded ** (Action Count)')

"""------------------------------------------------------------------------------------------------------------------------------------------
									 				  		Transfer Report Table	                 								       """

try:
	# Calculate excute time
	start_time = time.time()

	pattern = '*  Transfer Report (GA)_'+day+'*.csv'
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
		caller, sub_producer, transfer_date,date_added,transfer_type,id,milestone,lead_source,status,1st_vehicle_make,2nd_vehicle_make,3rd_vehicle_make,4th_vehicle_make) 
		SELECT t.caller, e.sub_producer, t.transfer_date,t.date_added,t.transfer_type,t.id,t.milestone,t.lead_source,t.status,t.1st_vehicle_make,t.2nd_vehicle_make,t.3rd_vehicle_make,t.4th_vehicle_make
		FROM transfer_temp AS t
		LEFT JOIN employee AS e
		ON (e.first_name = t.sales_first_name AND e.last_name = t.sales_last_name)
	""")

	conn.execute(""" DROP TABLE transfer_temp; """)
	file_name = pull_file_name(pattern)
	new_location = "S:/Data Team/Archive folder/Georgia Archive file/Georgia Transfers Archive/"+file_name
	file_location = pull_file_path(pattern)
	shutil.move(file_location, new_location)

	print('\n 8. "Transfers" insertion compelete after: {} seconds and moved to Archive!!'.format(time.time() - start_time))

except:
	print('\n 8. ERROR !!  ** File not uploaded ** (Transfers) ')



"""------------------------------------------------------------------------------------------------------------------------------------------
									 				  		Call History Table	                 								       """

try:
	os.chdir('S:/Data Team/DB Raw files/Dial IQ')
	path_file = os.getcwd()
	pattern = 'Lm30364_CallHistory_'+day+'*.csv'
	# Reading to df using function
	df = pull_pattern(pattern)

	df.columns = [c.lower() for c in df.columns]
	df.columns = [c.replace(" ", "_") for c in df.columns]

	df[['sales_last_name', 'sales_first_name']] = df['user'].str.split(", ", 1, True) # link with employee name 
	df[['first_name', 'last_name']] = df['lead'].str.split(" ", 1, True) # can't link, customer not in customer table yet
	df['call_duration'] = df['call_duration'].map(lambda x: str(x)[:-3])
	df['call_duration'] = df['call_duration'].map(lambda x: str(x).replace(":",'.'))
	df.drop(['lead','user','inbound_number', 'wait_time','target_number'], axis = 1 , inplace = True)
	df.rename(columns={'time':'call_date', 'group':'sales_group'}, inplace=True)
	df['sales_group'].fillna(value = 'Fl', inplace=True)
	df = df[df['sales_group'].str.contains("GA")]
	# Not working looking into it.  df[df['lead_id'] != '(N/A)'] 
	df = df[['lead_id','sales_first_name','sales_last_name','first_name','last_name', 'sales_group', 'call_segment', 'origin', 'result','prospect_number','lead_source',
			'call_duration','call_date']]
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
		lead_id, sub_producer,first_name,last_name,sales_group,call_segment,origin,result,prospect_number,lead_source,call_duration,call_date) 
		SELECT c.lead_id, e.sub_producer,c.first_name,c.last_name,c.sales_group,c.call_segment,c.origin,c.result,c.prospect_number,c.lead_source,c.call_duration,c.call_date 
		FROM call_history_temp AS c
		LEFT JOIN employee AS e
		ON (e.first_name = c.sales_first_name AND e.last_name = c.sales_last_name)
	"""
	)

	conn.execute(""" DROP TABLE call_history_temp; """)

	print('\n 9."Call History" insertion compelete after: {} seconds'.format(time.time() - start_time))

except:
	print('\n 9. ERROR !!  ** File not uploaded ** (Call History) ')

input('\n Press ENTER to exit')