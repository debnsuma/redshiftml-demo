--DEMO 2
--======

--Create Schema
CREATE SCHEMA DEMO_ML2;

--Create TABLE (Training)
CREATE TABLE demo_ml2.steel_plates_fault (
            X_Minimum	int,
            X_Maximum	int,
            Y_Minimum	int,
            Y_Maximum	int,
            Pixels_Areas	int,
            X_Perimeter	int,
            Y_Perimeter	int,
            Sum_of_Luminosity	int,
            Minimum_of_Luminosity	int,
            Maximum_of_Luminosity	int,
            Length_of_Conveyer	int,
            TypeOfSteel_A300	int,
            TypeOfSteel_A400	int,
            Steel_Plate_Thickness	int,
            Edges_Index	float,
            Empty_Index	float,
            Square_Index	float,
            Outside_X_Index	float,
            Edges_X_Index	float,
            Edges_Y_Index	float,
            Outside_Global_Index	float,
            LogOfAreas	float,
            Log_X_Index	float,
            Log_Y_Index	float,
            Orientation_Index	float,
            Luminosity_Index	float,
            SigmoidOfAreas	float,
            SteelType	varchar(120));

--Ingest (Training)
COPY demo_ml2.steel_plates_fault FROM 's3://redshift-downloads-2021/steel_fault_train.csv' IAM_ROLE 'arn:aws:iam::123456789123:role/RedshiftMLRole' delimiter ',' IGNOREHEADER 1;

--Create Table (Inference)
CREATE TABLE demo_ml2.steel_plates_fault_inference (
            X_Minimum	int,
            X_Maximum	int,
            Y_Minimum	int,
            Y_Maximum	int,
            Pixels_Areas	int,
            X_Perimeter	int,
            Y_Perimeter	int,
            Sum_of_Luminosity	int,
            Minimum_of_Luminosity	int,
            Maximum_of_Luminosity	int,
            Length_of_Conveyer	int,
            TypeOfSteel_A300	int,
            TypeOfSteel_A400	int,
            Steel_Plate_Thickness	int,
            Edges_Index	float,
            Empty_Index	float,
            Square_Index	float,
            Outside_X_Index	float,
            Edges_X_Index	float,
            Edges_Y_Index	float,
            Outside_Global_Index	float,
            LogOfAreas	float,
            Log_X_Index	float,
            Log_Y_Index	float,
            Orientation_Index	float,
            Luminosity_Index	float,
            SigmoidOfAreas	float,
            SteelType	varchar(120));

--Ingest (Training)
COPY demo_ml2.steel_plates_fault_inference FROM 's3://redshift-downloads-2021/steel_fault_test.csv' IAM_ROLE 'arn:aws:iam::123456789123:role/RedshiftMLRole' delimiter ',' IGNOREHEADER 1;

--Drop MODEL if exists
DROP MODEL IF EXISTS demo_ml2.func_model_steel_fault;

--Create MODEL
CREATE MODEL demo_ml2.func_model_steel_fault
FROM (
SELECT
    X_Minimum,
    X_Maximum,
    Y_Minimum,
    Y_Maximum,
    Pixels_Areas,
    X_Perimeter,
    Y_Perimeter,
    Sum_of_Luminosity,
    Minimum_of_Luminosity,
    Maximum_of_Luminosity,
    Length_of_Conveyer,
    TypeOfSteel_A300,
    TypeOfSteel_A400,
    Steel_Plate_Thickness,
    Edges_Index,
    Empty_Index,
    Square_Index,
    Outside_X_Index,
    Edges_X_Index,
    Edges_Y_Index,
    Outside_Global_Index,
    LogOfAreas,
    Log_X_Index,
    Log_Y_Index,
    Orientation_Index,
    Luminosity_Index,
    SigmoidOfAreas,
    SteelType
FROM demo_ml2.steel_plates_fault
)
TARGET SteelType
FUNCTION func_model_steel_fault IAM_ROLE 'arn:aws:iam::123456789123:role/RedshiftMLRole'
PROBLEM_TYPE multiclass_classification
OBJECTIVE 'accuracy'
SETTINGS(
    S3_BUCKET 'redshiftml-demo-bucket',
    max_runtime 1800
);

--Check the status of the Model
SELECT * FROM stv_ml_model_info;

--Show Model
SHOW MODEL ALL;

SHOW MODEL DEMO_ML2.func_model_steel_fault;

--Check Accuracy of the model `func_model_steel_fault`
WITH infer_data
AS (
	SELECT SteelType AS actual
		,DEMO_ML2.func_model_steel_fault(X_Minimum, X_Maximum, Y_Minimum, Y_Maximum, Pixels_Areas, X_Perimeter,
		    Y_Perimeter, Sum_of_Luminosity, Minimum_of_Luminosity, Maximum_of_Luminosity, Length_of_Conveyer,
		    TypeOfSteel_A300, TypeOfSteel_A400, Steel_Plate_Thickness, Edges_Index, Empty_Index, Square_Index,
		    Outside_X_Index, Edges_X_Index, Edges_Y_Index, Outside_Global_Index, LogOfAreas, Log_X_Index, Log_Y_Index,
		    Orientation_Index, Luminosity_Index, SigmoidOfAreas) AS predicted

        ,CASE WHEN actual is NULL THEN NULL ELSE actual END AS actual,
                CASE WHEN actual = predicted THEN 1::INT
                ELSE 0::INT END AS correct
    FROM DEMO_ML2.steel_plates_fault_inference
	)
	,aggr_data
AS (
	SELECT SUM(correct) AS num_correct
		,COUNT(*) AS total
	FROM infer_data
	)
SELECT (num_correct::FLOAT / total::FLOAT) AS accuracy
FROM aggr_data;




--Prediction
SELECT DEMO_ML2.func_model_steel_fault(X_Minimum, X_Maximum, Y_Minimum, Y_Maximum,
    Pixels_Areas, X_Perimeter, Y_Perimeter, Sum_of_Luminosity, Minimum_of_Luminosity,
    Maximum_of_Luminosity, Length_of_Conveyer, TypeOfSteel_A300, TypeOfSteel_A400, Steel_Plate_Thickness,
    Edges_Index, Empty_Index, Square_Index, Outside_X_Index, Edges_X_Index, Edges_Y_Index,
    Outside_Global_Index, LogOfAreas, Log_X_Index, Log_Y_Index,
    Orientation_Index, Luminosity_Index, SigmoidOfAreas)

FROM DEMO_ML2.steel_plates_fault_inference;

