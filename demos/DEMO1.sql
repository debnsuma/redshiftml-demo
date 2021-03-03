--DEMO 1
--======


--Create Schema
CREATE SCHEMA DEMO_ML;

--Create Table (Training)
CREATE TABLE DEMO_ML.client_details (
    age int,
    job varchar(120),
    marital varchar(120),
    education varchar(120),
    "default" varchar(120),
    housing varchar(120),
    loan varchar(120),
    contact varchar(120),
    month varchar(120),
    day_of_week varchar(10),
    duration int,
    campaign int,
    pdays int,
    previous int,
    poutcome varchar(120),
    emp_var_rate float,
    cons_price_idx float,
    cons_conf_idx float,
    euribor3m float,
    nr_employed float,
    y varchar);

--Ingest (Training)
COPY demo_ml.client_details FROM 's3://redshift-downloads-2021/bank-additional-full.csv' IAM_ROLE 'arn:aws:iam::123456789123:role/RedshiftMLRole' delimiter ',' IGNOREHEADER 1;

--Create Table (Inference)
CREATE TABLE DEMO_ML.client_details_inference (
    age int,
    job varchar(120),
    marital varchar(120),
    education varchar(120),
    "default" varchar(120),
    housing varchar(120),
    loan varchar(120),
    contact varchar(120),
    month varchar(120),
    day_of_week varchar(10),
    duration int,
    campaign int,
    pdays int,
    previous int,
    poutcome varchar(120),
    emp_var_rate float,
    cons_price_idx float,
    cons_conf_idx float,
    euribor3m float,
    nr_employed float,
    y varchar);

--Ingest (Training)
COPY demo_ml.client_details_inference FROM 's3://redshift-downloads-2021/bank-additional-inference.csv' IAM_ROLE 'arn:aws:iam::123456789123:role/RedshiftMLRole' delimiter ',' IGNOREHEADER 1;

--Create USERS and GROUPS
CREATE GROUP dbdev_group;

CREATE USER dbdev_user PASSWORD 'Password1234' IN GROUP dbdev_group;

CREATE GROUP datascience_group;

CREATE USER datascience_user PASSWORD 'Password1234' IN GROUP datascience_group;

--Grant customer "client_details" table the permission to both the USER GROUPS
GRANT CREATE, USAGE ON SCHEMA demo_ml TO GROUP datascience_group;

GRANT ALL ON ALL TABLES IN SCHEMA demo_ml TO GROUP datascience_group;

GRANT ALL ON ALL TABLES IN SCHEMA demo_ml TO GROUP dbdev_group;

GRANT SELECT ON TABLE demo_ml.client_details TO GROUP dbdev_group;

GRANT SELECT ON TABLE demo_ml.client_details_inference TO GROUP dbdev_group;

GRANT USAGE ON SCHEMA demo_ml TO GROUP dbdev_group;

--Grant CREATE MODEL Permission only to the "datascience_group"
GRANT CREATE MODEL TO GROUP datascience_group;

--Change user to "datascience_user" user
set session AUTHORIZATION awsuser;

--Drop MODEL if exists
DROP MODEL IF EXISTS demo_ml.func_model_bank_marketing;

--Create MODEL
CREATE MODEL demo_ml.func_model_bank_marketing
FROM (SELECT age
        ,job
        ,marital
        ,education
        ,"default"
        ,housing
        ,loan
        ,contact
        ,month
        ,day_of_week
        ,duration
        ,campaign
        ,pdays
        ,previous
        ,poutcome
        ,emp_var_rate
        ,cons_price_idx
        ,cons_conf_idx
        ,euribor3m
        ,nr_employed
        ,y
    FROM demo_ml.client_details
     )
TARGET y
FUNCTION func_model_bank_marketing
IAM_ROLE 'arn:aws:iam::123456789123:role/RedshiftMLRole'
SETTINGS (
    S3_BUCKET 'redshiftml-demo-bucket',
    max_runtime 1800
);

--Check the status of the Model
SELECT * FROM stv_ml_model_info;

--Show Model
SHOW MODEL ALL;

SHOW MODEL DEMO_ML.func_model_bank_marketing;

--Grant CREATE EXECUTE Permission only to the "database-admin group"
GRANT EXECUTE ON MODEL demo_ml.func_model_bank_marketing TO GROUP dbdev_group;

--Change user to "dbdev_user" user
set session AUTHORIZATION dbdev_user;

--Check Accuracy of the model `model_bank_marketing`
WITH infer_data
AS (
	SELECT y AS actual
		,demo_ml.func_model_bank_marketing2(age, job, marital, education, "default",
		    housing, loan, contact, month, day_of_week, duration, campaign, pdays, previous,
		    poutcome, emp_var_rate, cons_price_idx, cons_conf_idx,
		    euribor3m, nr_employed) AS predicted
		,CASE
			WHEN actual = predicted
				THEN 1::INT
			ELSE 0::INT
			END AS correct
	FROM DEMO_ML.client_details_inference
	)
	,aggr_data
AS (
	SELECT SUM(correct) AS num_correct
		,COUNT(*) AS total
	FROM infer_data
	)
SELECT (num_correct::FLOAT / total::FLOAT) AS accuracy
FROM aggr_data;

--Predict how many customers will subscribe for term deposit vs not subscribe
WITH term_data AS ( SELECT demo_ml.func_model_bank_marketing( age,job,marital,education,"default",housing,loan,contact,month,day_of_week,duration,campaign,pdays,previous,poutcome,emp_var_rate,cons_price_idx,cons_conf_idx,euribor3m,nr_employed) AS predicted
FROM DEMO_ML.client_details_inference )
SELECT
CASE WHEN predicted = 'yes'  THEN 'Yes-will-do-a-term-deposit'
     WHEN predicted = 'no'  THEN 'No-term-deposit'
     ELSE 'Neither' END as deposit_prediction,
COUNT(1) AS count
from term_data GROUP BY 1;

