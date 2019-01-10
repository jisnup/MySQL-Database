from sqlalchemy import create_engine
import mysql.connector
import pandas as pd
import time 

start_time = time.time()
engine = create_engine('mysql+mysqlconnector://root:TTIdb@localhost:3306/sample', echo = False)
conn = engine.connect()

df = pd.read_excel('C:/Users/jisnu/Desktop/jisnu/data/Employee_Details.xlsx')

df.columns = [c.lower() for c in df.columns]
df.columns = [c.replace(" ", "_") for c in df.columns]
df.drop(['state','220','215', 'termination_date'], axis = 1, inplace  = True)
df.rename(columns={'license_#':'license', 'life_writing_#' :'life_writing'}, inplace=True)
df = df[['license', 'first_name','last_name', 'hire_date','birthday', 'gender', 'position','department', 'office_location' ,'payroll_department' ,'source' ,'sub_code' ,'life_writing',
        'shirt_size' ,'recruited' ,'previously_licensed', 'status']]
df.dropna(axis=0, how='all', thresh=None, subset=None, inplace=True)
df.to_sql(name='employee_temp', con= conn, if_exists = 'append', index = False)

df2 = pd.read_excel('C:/Users/jisnu/Desktop/jisnu/data/Terminated Employees.xlsx')
df2.drop([220,215], axis = 1, inplace  = True)
df2.columns = [c.lower() for c in df2.columns]
df2.columns = [c.replace(" ", "_") for c in df2.columns]
df2.drop(['state','remove_file','voluntary', 'reason','termination_date'], axis = 1, inplace  = True)
df2.rename(columns={'license_#':'license', 'life_writing_#' :'life_writing'}, inplace=True)
df2 = df2[['license', 'first_name','last_name', 'hire_date','birthday', 'gender', 'position','department', 'office_location' ,'payroll_department' ,'source' ,'sub_code' ,'life_writing',
        'shirt_size' ,'recruited' ,'previously_licensed', 'status']]
df2.dropna(axis=0, how='all', thresh=None, subset=None, inplace=True)

df2.to_sql(name='employee_temp', con= conn, if_exists = 'append', index = False)

conn.execute(
"""
CREATE TABLE IF NOT EXISTS employee_temp(
    license VARCHAR(20),
    first_name VARCHAR(20),
    last_name VARCHAR(20),
    hire_date VARCHAR(25),
    birthday VARCHAR(25),
    gender CHAR(6),
    position VARCHAR(50),
    department VARCHAR(20),
    office_location VARCHAR(20),
    payroll_department VARCHAR(20),
    source VARCHAR(20),
    sub_code INT,
    life_writing VARCHAR(20),  
    shirt_size VARCHAR(20),
    recruited CHAR(10),
    previously_licensed CHAR(5),
    status VARCHAR(20));
""")

conn.execute(
"""
    INSERT INTO employee(
    license, first_name,last_name, hire_date,birthday, gender, position,department, office_location ,payroll_department ,source ,sub_code ,life_writing ,shirt_size ,recruited,
    previously_licensed,status) 
    SELECT DISTINCT license, first_name,last_name, hire_date,birthday, gender, position,department, office_location ,payroll_department ,source ,sub_code ,life_writing ,shirt_size,
    recruited ,previously_licensed, status
     FROM employee_temp 
    WHERE employee_temp.license NOT IN (SELECT license FROM employee)   
""")

conn.execute(""" DROP TABLE employee_temp; """)

print('Insert compelete after: {} seconds'.format(time.time()- start_time))

#Done
