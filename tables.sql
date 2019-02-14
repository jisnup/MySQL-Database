CREATE TABLE IF NOT EXISTS employee
(
    license VARCHAR(20) PRIMARY KEY,
    first_name VARCHAR(20) NOT NULL,
    last_name VARCHAR(20) NOT NULL,
    hire_date VARCHAR(20),
    birthday VARCHAR(20),
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
    status VARCHAR(20))
    ENGINE=INNODB;

INSERT INTO employee 
VALUES('Missing','No','Code','xx/xx/xxxx','xx/xx/xxxx','x','xxxxx','xxx','location','xxx','xxx',000,'','','','','Active');

SELECT * FROM employee
ORDER BY last_name
TRUNCATE TABLE employee;

CREATE TABLE IF NOT EXISTS customer(
    customer_id INT AUTO_INCREMENT,
    first_name VARCHAR(20) NOT NULL,
    last_name VARCHAR(20),
    dob VARCHAR(20),
    gender CHAR(6),
    phone VARCHAR(300),
    email_id VARCHAR(320) NOT NULL,
    marital_status VARCHAR(20),
    address VARCHAR(300),
    city VARCHAR(50),
    state VARCHAR(25),
    zipcode VARCHAR(10),
    easy_pay VARCHAR(15),
    e_policy VARCHAR(15),
    e_bill VARCHAR(15),
    UNIQUE KEY(customer_id),
    PRIMARY KEY(phone,email_id))
    ENGINE=INNODB;

SELECT * FROM customer;
DROP TABLE IF EXISTS customer;
TRUNCATE TABLE

CREATE TABLE IF NOT EXISTS product(
    line_code VARCHAR(20) PRIMARY KEY,
    product VARCHAR(50))
    ENGINE=INNODB;

SELECT * FROM product;
DROP TABLE IF EXISTS product;
TRUNCATE TABLE

CREATE TABLE IF NOT EXISTS fl_quotes(
	quote_number VARCHAR(50),
    license VARCHAR(50),
	customer_first_name VARCHAR(50),
	customer_last_name VARCHAR(50),
	production_date VARCHAR(30),
	product VARCHAR(50),
	quoted_item_count INT,
	quoted_premium FLOAT(8,2),
	PRIMARY KEY(quote_number, production_date),
	FOREIGN KEY (license) REFERENCES employee(license) ON UPDATE CASCADE)
    ENGINE=INNODB;    
    
SELECT * FROM fl_quotes
DROP TABLE IF EXISTS fl_quotes;
TRUNCATE TABLE

CREATE TABLE IF NOT EXISTS fl_sales(
    policy_no VARCHAR(40),
    license VARCHAR(50),
    customer_last_name VARCHAR(50),
    customer_first_name VARCHAR(50),
    line_code VARCHAR(35),
    product VARCHAR(30),
    package VARCHAR(55),
    item_count INT,
    disposition_code VARCHAR(35),
    transaction_type VARCHAR(35),
    issued_date VARCHAR(35),
    date_written VARCHAR(35),
    written_premium FLOAT(8,2),
    PRIMARY KEY(policy_no, disposition_code, transaction_type, issued_date, written_premium),
    FOREIGN KEY (license) REFERENCES employee(license) ON UPDATE CASCADE)
    ENGINE=INNODB;

SELECT * FROM fl_sales
ORDER BY issued_date
DROP TABLE IF EXISTS fl_sales;
TRUNCATE TABLE

CREATE TABLE IF NOT EXISTS termination(
    policy_number VARCHAR(30) PRIMARY KEY,
    original_year INT,
    termination_effective_date DATE,
    termination_reason VARCHAR(500),
    premium_new FLOAT(8,2),
    premium_old FLOAT(8,2),
    number_of_items INT,
    FOREIGN KEY (policy_number) REFERENCES fl_sales(policy_no) ON UPDATE CASCADE)
    ENGINE = INNODB;

SELECT * FROM termination;
 
DROP TABLE IF EXISTS termination;
TRUNCATE TABLE

CREATE TABLE IF NOT EXISTS call_history(
	lead_id VARCHAR(50),
	license VARCHAR(30),
	first_name VARCHAR(50),
	last_name VARCHAR(50),
	sales_group VARCHAR(50),
	call_segment VARCHAR(30),
	origin VARCHAR(30),
	result VARCHAR(40),
	prospect_number VARCHAR(350),
	lead_source VARCHAR(40),
	call_duration VARCHAR(30),
	call_date VARCHAR(50),
	PRIMARY KEY(lead_id),
	FOREIGN KEY (license) REFERENCES employee(license) ON UPDATE CASCADE)
    ENGINE=INNODB;

ALTER TABLE call_history
ADD FOREIGN KEY (license) REFERENCES employee(license) ON UPDATE CASCADE; 


SELECT * FROM call_history;
TRUNCATE TABLE call_history;

CREATE TABLE IF NOT EXISTS action_count(
	license VARCHAR(50),
	total_action VARCHAR(30),
	action_date VARCHAR(30),
	FOREIGN KEY (license) REFERENCES employee(license) ON UPDATE CASCADE)
    ENGINE=INNODB;

SELECT * FROM action_count;
DROP TABLE action_count;
TRUNCATE TABLE

CREATE TABLE IF NOT EXISTS lead_life(
	license VARCHAR(50),
	log_type VARCHAR(20),
	id VARCHAR(30),
	lead_source VARCHAR(30),
	log_date VARCHAR(30),
	date_added VARCHAR(30),
	PRIMARY KEY(id,log_date),
	FOREIGN KEY (license) REFERENCES employee(license) ON UPDATE CASCADE)
    ENGINE=INNODB;

ALTER TABLE lead_life
ADD FOREIGN KEY (license) REFERENCES employee(license) ON UPDATE CASCADE;

SELECT * FROM lead_life;
DROP TABLE lead_life;
TRUNCATE TABLE

CREATE TABLE IF NOT EXISTS transfer(
	caller VARCHAR(30),
	license VARCHAR(30),
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
	4th_vehicle_make VARCHAR(30),
	FOREIGN KEY (license) REFERENCES employee(license) ON UPDATE CASCADE,
	FOREIGN KEY (caller) REFERENCES employee(license) ON UPDATE CASCADE)
    ENGINE=INNODB;

ALTER TABLE transfer
ADD FOREIGN KEY (license) REFERENCES employee(license);

ALTER TABLE transfer
ADD FOREIGN KEY (caller) REFERENCES employee(license);
    
SELECT * FROM transfer;
DROP TABLE transfer;
TRUNCATE TABLE transfer;

CREATE TABLE IF NOT EXISTS past_x_dates(
	license VARCHAR(30),
	id VARCHAR(30),
	lead_source VARCHAR(30),
	lead_score VARCHAR(30),
	date_added VARCHAR(30),
	last_action_date VARCHAR(30),
	status VARCHAR(30),
	x_date VARCHAR(30),
	flagged VARCHAR(30),
	PRIMARY KEY(id, last_action_date),
	FOREIGN KEY (license) REFERENCES employee(license) ON UPDATE CASCADE)
    ENGINE=INNODB;

ALTER TABLE past_x_dates
ADD FOREIGN KEY (license) REFERENCES employee(license);

SELECT * FROM past_x_dates;
DROP TABLE past_x_dates;
TRUNCATE TABLE past_x_dates;

CREATE TABLE IF NOT EXISTS past_follow_ups(
	license VARCHAR(30),
	id VARCHAR(30),
	lead_source VARCHAR(30),
	lead_score VARCHAR(30),
	date_added VARCHAR(30),
	last_action_date VARCHAR(30),
	status VARCHAR(30),
	follow_up_date VARCHAR(30),
	flagged VARCHAR(30),
	PRIMARY KEY(id, last_action_date),
	FOREIGN KEY (license) REFERENCES employee(license) ON UPDATE CASCADE)
    ENGINE=INNODB;

ALTER TABLE past_follow_ups
ADD FOREIGN KEY (license) REFERENCES employee(license);

SELECT * FROM past_follow_ups;
DROP TABLE past_follow_ups;
TRUNCATE TABLE past_follow_ups;

SHOW TABLES;
SELECT * FROM employee;
SELECT * FROM department;
SELECT * FROM locations;
SELECT * FROM business;
SELECT * FROM customer;
SELECT * FROM fl_quotes;
SELECT * FROM fl_sales;
SELECT * FROM termination;
SELECT * FROM product;

DROP TABLE IF EXISTS employee;
DROP TABLE IF EXISTS department;
DROP TABLE IF EXISTS department_temp;
DROP TABLE IF EXISTS business;
DROP TABLE IF EXISTS location_temp;
DROP TABLE IF EXISTS location;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS fl_quotes;
DROP TABLE IF EXISTS fl_sales_new;
DROP TABLE IF EXISTS termination;
DROP TABLE IF EXISTS product;
DROP TABLE IF EXISTS fl_sales_temp;
DROP TABLE IF EXISTS fl_sales_cancel;

CREATE OR REPLACE VIEW emp_name AS
SELECT position_id,  first_name,last_name AS employee_name 
FROM employee

select * from emp_name
DROP VIEW IF EXISTS emp_name;

SET FOREIGN_KEY_CHECKS = 1;
SET GLOBAL innodb_fast_shutdown = 1;

SET GLOBAL innodb_fast_shutdown = 0;
SET FOREIGN_KEY_CHECKS = 0;

SET wait_timeout = 100000; 
set global max_allowed_packet=67108864;
SHOW VARIABLES WHERE Variable_name = 'hostname';

SHOW TABLES;

CREATE TABLE IF NOT EXISTS jornaya(
	recordid VARCHAR(250),
	phone01 VARCHAR(300),
	phone02 VARCHAR(300),
	phone03 VARCHAR(300),
	phone04 VARCHAR(300),
	email01 VARCHAR(100),
	email02 VARCHAR(100),
	email03 VARCHAR(100),
	leadid01 VARCHAR(250),
	leadid02 VARCHAR(250),
	leadid03 VARCHAR(250))
    ENGINE=INNODB;

SELECT recordid, sha2(phone01, 256) as phone01, phone02, phone03, phone04, sha2(email01, 256) as email01, email02, email03, leadid01, leadid02, leadid03
FROM jornaya;

SELECT * FROM jornaya

DROP TABLE jornaya;

SHOW Tables;

SELECT * FROM fl_quotes_temp;                       