--DEMO 3
--======

--Create Schema
CREATE SCHEMA DEMO_ML3;

--Create TABLE (Training for xgboost model training)
CREATE TABLE DEMO_ML3.abalone_xgb_train (
                length_val float,
                diameter float,
                height float,
                whole_weight float,
                shucked_weight float,
                viscera_weight float,
                shell_weight float,
                rings int
                );

--Ingest (Training)
COPY DEMO_ML3.abalone_xgb_train FROM 's3://redshift-downloads-2021/xgboost_abalone_train.csv' IAM_ROLE 'arn:aws:iam::123456789123:role/RedshiftMLRole' delimiter ',' IGNOREHEADER 1;

--Create TABLE (Testing/Inference)
CREATE TABLE DEMO_ML3.abalone_xgb_test (
                length_val float,
                diameter float,
                height float,
                whole_weight float,
                shucked_weight float,
                viscera_weight float,
                shell_weight float,
                rings int
                );

--Ingest (Testing)
COPY DEMO_ML3.abalone_xgb_test FROM 's3://redshift-downloads-2021/xgboost_abalone_test.csv' IAM_ROLE 'arn:aws:iam::123456789123:role/RedshiftMLRole' delimiter ',' IGNOREHEADER 1;

-- Create MODEL
CREATE MODEL DEMO_ML3.model_abalone_xgboost_regression
FROM (SELECT
      length_val,
      diameter,
      height,
      whole_weight,
      shucked_weight,
      viscera_weight,
      shell_weight,
      rings
     FROM DEMO_ML3.abalone_xgb_train)
TARGET Rings
FUNCTION func_model_abalone_xgboost_regression
IAM_ROLE 'arn:aws:iam::123456789123:role/RedshiftMLRole'
AUTO OFF
MODEL_TYPE xgboost
OBJECTIVE 'reg:squarederror'
PREPROCESSORS 'none'
HYPERPARAMETERS DEFAULT EXCEPT (NUM_ROUND '100')
SETTINGS (S3_BUCKET 'redshiftml-demo-bucket');

--Show MODEL
SHOW MODEL ALL;


SHOW MODEL DEMO_ML3.model_abalone_xgboost_regression;

--Accuracy
WITH infer_data AS (
    SELECT rings AS label, DEMO_ML3.func_model_abalone_xgboost_regression(Length_val, Diameter, Height,
                                            Whole_weight, Shucked_weight, Viscera_weight, Shell_weight) AS predicted,
    CASE WHEN label is NULL THEN 0 ELSE label END AS actual
    FROM DEMO_ML3.abalone_xgb_test
)
SELECT SQRT(AVG(POWER(actual - predicted, 2))) AS rmse FROM infer_data;

--Prediction
WITH age_data AS ( SELECT DEMO_ML3.func_model_abalone_xgboost_regression( length_val,
                                               diameter,
                                               height,
                                               whole_weight,
                                               shucked_weight,
                                               viscera_weight,
                                               shell_weight ) + 1.5 AS age
FROM DEMO_ML3.abalone_xgb_test )
SELECT
CASE WHEN age  > 20 THEN 'age_over_20'
     WHEN age  > 10 THEN 'age_between_10_20'
     WHEN age  > 5  THEN 'age_between_5_10'
     ELSE 'age_5_and_under' END as age_group,
COUNT(1) AS count
from age_data GROUP BY 1;
