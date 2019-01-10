from sqlalchemy import create_engine
import mysql.connector
import pandas as pd
import time

start_time = time.time()

engine = create_engine('mysql+mysqlconnector://root:TTIdb@localhost:3306/sample', echo = False)
conn = engine.connect()

conn.execute("""
ALTER TABLE customer ADD tokeep boolean;
ALTER TABLE customer ADD CONSTRAINT preventdupe unique (first_name,last_name,phone,email_id, tokeep);
UPDATE ignore customer SET tokeep = true;
DELETE FROM customer WHERE tokeep IS NULL;
ALTER TABLE customer DROP tokeep;
""")
print('Insert compelete after: {} seconds'.format(time.time() - start_time))