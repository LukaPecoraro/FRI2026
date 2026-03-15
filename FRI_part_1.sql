-- basic things

CREATE WAREHOUSE IF NOT EXISTS XS_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

USE WAREHOUSE XS_WH;

-- security ROLES
-- very important, but not for demo purposes
-- USE ROLE ACCOUNTADMIN
-- best practise to use SYSADMIN for creating objects and SECURITYADMIN for user and roles
-- in real projects you have to spent time to design the functional and technical roles to support your environment and NEVER use accountadmin for everything

-- basic objects

--CREATE DATABASE MY_DATA_LAKE;
USE DATABASE MY_DATA_LAKE;

CREATE SCHEMA BRONZE;
CREATE SCHEMA SILVER;
CREATE SCHEMA GOLD;

USE SCHEMA BRONZE;


-- for external buckets like S3, Azure blob, Google, you need to create a storage integration, but since we have a public S3 bucket, we will directly create a STAGE object

-- CREATE OR REPLACE STORAGE INTEGRATION s3_data_integration
--   TYPE = EXTERNAL_STAGE
--   STORAGE_PROVIDER = 'S3'
--   STORAGE_AWS_ROLE_ARN = 'AWS_ROLE'
--   ENABLED = TRUE
--   STORAGE_ALLOWED_LOCATIONS = ('s3://s3-taxi-fri-demo/');

CREATE OR REPLACE STAGE my_public_nyc_stage
  URL = 's3://s3-taxi-fri-demo/'
  -- No credentials needed for public buckets
  DIRECTORY = (ENABLE = TRUE);

LIST @my_public_nyc_stage;

-- we need to create a file format to specify the type of external data (csv, parquet, avro,...)
CREATE FILE FORMAT IF NOT EXISTS parquet_nyc_format TYPE = 'PARQUET';
CREATE FILE FORMAT IF NOT EXISTS csv_nyc_format TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"';

-- Snowflake supports also infer schema option to read the structure from the parquet file
-- CREATE OR REPLACE TABLE MY_DATA_LAKE.BRONZE.TRIP_DATA
--   USING TEMPLATE (
--     SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
--     FROM TABLE(INFER_SCHEMA(
--       LOCATION => '@MY_DATA_LAKE.BRONZE.my_public_nyc_stage/yellow_tripdata_2025-01.parquet',
--       FILE_FORMAT => 'MY_DATA_LAKE.BRONZE.parquet_nyc_format'
--     ))
--   );

-- if you need to get any DDL (data definition language) of an object you can use this 
-- SELECT GET_DDL('TABLE','TRIP_DATA');

CREATE OR REPLACE TABLE BRONZE.TRIP_DATA (
	VendorID NUMBER(38,0),
	tpep_pickup_datetime TIMESTAMP,
	tpep_dropoff_datetime TIMESTAMP,
	passenger_count NUMBER(38,0),
	trip_distance FLOAT,
	RatecodeID NUMBER(38,0),
	store_and_fwd_flag VARCHAR,
	PULocationID NUMBER(38,0),
	DOLocationID NUMBER(38,0),
	payment_type NUMBER(38,0),
	fare_amount FLOAT,
	extra FLOAT,
	mta_tax FLOAT,
	tip_amount FLOAT,
	tolls_amount FLOAT,
	improvement_surcharge FLOAT,
	total_amount FLOAT,
	congestion_surcharge FLOAT,
	Airport_fee FLOAT,
	cbd_congestion_fee FLOAT,
-- ADM fields
    ADM_FILENAME varchar
);

-- adding meta data info

ALTER TABLE MY_DATA_LAKE.BRONZE.TRIP_DATA MODIFY COLUMN VENDORID COMMENT 'A code indicating the TPEP provider: 1= Creative Mobile Technologies, LLC; 2= Curb Mobility, LLC; 6= Myle Technologies Inc; 7= Helix.';
ALTER TABLE MY_DATA_LAKE.BRONZE.TRIP_DATA MODIFY COLUMN TPEP_PICKUP_DATETIME COMMENT 'The date and time when the meter was engaged.';
ALTER TABLE MY_DATA_LAKE.BRONZE.TRIP_DATA MODIFY COLUMN TPEP_DROPOFF_DATETIME COMMENT 'The date and time when the meter was disengaged.';
ALTER TABLE MY_DATA_LAKE.BRONZE.TRIP_DATA MODIFY COLUMN PASSENGER_COUNT COMMENT 'The number of passengers in the vehicle.';
ALTER TABLE MY_DATA_LAKE.BRONZE.TRIP_DATA MODIFY COLUMN TRIP_DISTANCE COMMENT 'The elapsed trip distance in miles reported by the taximeter.';
ALTER TABLE MY_DATA_LAKE.BRONZE.TRIP_DATA MODIFY COLUMN RATECODEID COMMENT 'The final rate code in effect: 1= Standard; 2= JFK; 3= Newark; 4= Nassau/Westchester; 5= Negotiated; 6= Group; 99= Unknown.';
ALTER TABLE MY_DATA_LAKE.BRONZE.TRIP_DATA MODIFY COLUMN STORE_AND_FWD_FLAG COMMENT 'Indicates if the record was held in vehicle memory (Y= store and forward, N= not).';
ALTER TABLE MY_DATA_LAKE.BRONZE.TRIP_DATA MODIFY COLUMN PULOCATIONID COMMENT 'TLC Taxi Zone in which the taximeter was engaged.';
ALTER TABLE MY_DATA_LAKE.BRONZE.TRIP_DATA MODIFY COLUMN DOLOCATIONID COMMENT 'TLC Taxi Zone in which the taximeter was disengaged.';
ALTER TABLE MY_DATA_LAKE.BRONZE.TRIP_DATA MODIFY COLUMN PAYMENT_TYPE COMMENT 'Numeric code for payment: 0= Flex Fare; 1= Credit card; 2= Cash; 3= No charge; 4= Dispute; 5= Unknown; 6= Voided.';
ALTER TABLE MY_DATA_LAKE.BRONZE.TRIP_DATA MODIFY COLUMN FARE_AMOUNT COMMENT 'The time-and-distance fare calculated by the meter.';
ALTER TABLE MY_DATA_LAKE.BRONZE.TRIP_DATA MODIFY COLUMN EXTRA COMMENT 'Miscellaneous extras and surcharges.';
ALTER TABLE MY_DATA_LAKE.BRONZE.TRIP_DATA MODIFY COLUMN MTA_TAX COMMENT 'Tax automatically triggered based on the metered rate in use.';
ALTER TABLE MY_DATA_LAKE.BRONZE.TRIP_DATA MODIFY COLUMN TIP_AMOUNT COMMENT 'Tip amount (automatically populated for credit card tips; does not include cash).';
ALTER TABLE MY_DATA_LAKE.BRONZE.TRIP_DATA MODIFY COLUMN TOLLS_AMOUNT COMMENT 'Total amount of all tolls paid in trip.';
ALTER TABLE MY_DATA_LAKE.BRONZE.TRIP_DATA MODIFY COLUMN IMPROVEMENT_SURCHARGE COMMENT 'Improvement surcharge assessed at the flag drop (began in 2015).';
ALTER TABLE MY_DATA_LAKE.BRONZE.TRIP_DATA MODIFY COLUMN TOTAL_AMOUNT COMMENT 'The total amount charged to passengers (excludes cash tips).';
ALTER TABLE MY_DATA_LAKE.BRONZE.TRIP_DATA MODIFY COLUMN CONGESTION_SURCHARGE COMMENT 'Total amount collected in trip for NYS congestion surcharge.';
ALTER TABLE MY_DATA_LAKE.BRONZE.TRIP_DATA MODIFY COLUMN AIRPORT_FEE COMMENT 'For pick up only at LaGuardia and John F. Kennedy Airports.';
ALTER TABLE MY_DATA_LAKE.BRONZE.TRIP_DATA MODIFY COLUMN CBD_CONGESTION_FEE COMMENT 'Per-trip charge for MTA Congestion Relief Zone starting Jan. 5, 2025.'
;

DESCRIBE TABLE TRIP_DATA;
;

TRUNCATE TABLE BRONZE.TRIP_DATA;

-- if you want to resize the warehouse on the fly
--ALTER WAREHOUSE XS_WH SET WAREHOUSE_SIZE = 'XSMALL';

-- INSERT statement
INSERT INTO BRONZE.TRIP_DATA (
  VENDORID, TPEP_PICKUP_DATETIME, TPEP_DROPOFF_DATETIME, PASSENGER_COUNT,
  TRIP_DISTANCE, RATECODEID, STORE_AND_FWD_FLAG, PULOCATIONID, DOLOCATIONID,
  PAYMENT_TYPE, FARE_AMOUNT, EXTRA, MTA_TAX, TIP_AMOUNT, TOLLS_AMOUNT,
  IMPROVEMENT_SURCHARGE, TOTAL_AMOUNT, CONGESTION_SURCHARGE, AIRPORT_FEE,
  CBD_CONGESTION_FEE, ADM_FILENAME
)
SELECT 
  $1:VendorID::NUMBER(38,0),
  TO_TIMESTAMP($1:tpep_pickup_datetime::NUMBER(38,0), 6),
  TO_TIMESTAMP($1:tpep_dropoff_datetime::NUMBER(38,0), 6),
  $1:passenger_count::NUMBER(38,0),
  $1:trip_distance::FLOAT,
  $1:RatecodeID::NUMBER(38,0),
  $1:store_and_fwd_flag::VARCHAR,
  $1:PULocationID::NUMBER(38,0),
  $1:DOLocationID::NUMBER(38,0),
  $1:payment_type::NUMBER(38,0),
  $1:fare_amount::FLOAT,
  $1:extra::FLOAT,
  $1:mta_tax::FLOAT,
  $1:tip_amount::FLOAT,
  $1:tolls_amount::FLOAT,
  $1:improvement_surcharge::FLOAT,
  $1:total_amount::FLOAT,
  $1:congestion_surcharge::FLOAT,
  $1:Airport_fee::FLOAT,
  $1:cbd_congestion_fee::FLOAT,
  METADATA$FILENAME
FROM @MY_DATA_LAKE.BRONZE.my_public_nyc_stage/yellow_tripdata_2025-01.parquet
(
  FILE_FORMAT => 'parquet_nyc_format'
);


SELECT *
FROM BRONZE.TRIP_DATA
LIMIT 100;

SELECT 
    CASE VENDORID
        WHEN 1 THEN 'Creative Mobile Technologies, LLC'
        WHEN 2 THEN 'Curb Mobility, LLC'
        WHEN 6 THEN 'Myle Technologies Inc'
        WHEN 7 THEN 'Helix'
        ELSE 'Unknown'
    END AS vendor_name,
    TPEP_PICKUP_DATETIME AS pickup_datetime,
    TPEP_DROPOFF_DATETIME AS dropoff_datetime,
    PASSENGER_COUNT,
    TRIP_DISTANCE,
    TOTAL_AMOUNT
FROM BRONZE.TRIP_DATA;

--- load all files with pattern and COPY INTO

TRUNCATE TABLE BRONZE.TRIP_DATA;

COPY INTO BRONZE.TRIP_DATA (
    VENDORID, TPEP_PICKUP_DATETIME, TPEP_DROPOFF_DATETIME, PASSENGER_COUNT,
    TRIP_DISTANCE, RATECODEID, STORE_AND_FWD_FLAG, PULOCATIONID, DOLOCATIONID,
    PAYMENT_TYPE, FARE_AMOUNT, EXTRA, MTA_TAX, TIP_AMOUNT, TOLLS_AMOUNT,
    IMPROVEMENT_SURCHARGE, TOTAL_AMOUNT, CONGESTION_SURCHARGE, AIRPORT_FEE,
    CBD_CONGESTION_FEE, ADM_FILENAME
)
FROM (
    SELECT 
        $1:VendorID::NUMBER(38,0),
        TO_TIMESTAMP($1:tpep_pickup_datetime::NUMBER(38,0), 6),
        TO_TIMESTAMP($1:tpep_dropoff_datetime::NUMBER(38,0), 6),
        $1:passenger_count::NUMBER(38,0),
        $1:trip_distance::FLOAT,
        $1:RatecodeID::NUMBER(38,0),
        $1:store_and_fwd_flag::VARCHAR,
        $1:PULocationID::NUMBER(38,0),
        $1:DOLocationID::NUMBER(38,0),
        $1:payment_type::NUMBER(38,0),
        $1:fare_amount::FLOAT,
        $1:extra::FLOAT,
        $1:mta_tax::FLOAT,
        $1:tip_amount::FLOAT,
        $1:tolls_amount::FLOAT,
        $1:improvement_surcharge::FLOAT,
        $1:total_amount::FLOAT,
        $1:congestion_surcharge::FLOAT,
        $1:Airport_fee::FLOAT,
        $1:cbd_congestion_fee::FLOAT,
        METADATA$FILENAME
    FROM @MY_DATA_LAKE.BRONZE.MY_PUBLIC_NYC_STAGE/
)
FILE_FORMAT = (TYPE = 'PARQUET')
PATTERN = '.*\.parquet';

SELECT 
  YEAR(tpep_pickup_datetime) AS year, 
  MONTH(tpep_pickup_datetime) as month, 
  COUNT(*)
FROM BRONZE.TRIP_DATA
GROUP BY year, month;

SELECT *
FROM BRONZE.TRIP_DATA
WHERE YEAR(tpep_pickup_datetime) < 2025;

-- delete problematic data
DELETE FROM BRONZE.TRIP_DATA
WHERE YEAR(tpep_pickup_datetime) < 2025;

------------------------------
-- CREATE & LOAD a LOOKUP Table
------------------------------

CREATE OR REPLACE TABLE BRONZE.TAXI_ZONE_LOOKUP (
    LocationID INTEGER,
    Borough VARCHAR,
    Zone VARCHAR,
    service_zone VARCHAR
);

COPY INTO BRONZE.TAXI_ZONE_LOOKUP
FROM @MY_DATA_LAKE.BRONZE.MY_PUBLIC_NYC_STAGE/taxi_zone_lookup.csv
FILE_FORMAT = csv_nyc_format;

SELECT *
FROM BRONZE.TAXI_ZONE_LOOKUP
LIMIT 100;

-- get locations with simple joins

SELECT 
    t.VENDORID,
    t.TPEP_PICKUP_DATETIME AS pickup_datetime,
    t.TPEP_DROPOFF_DATETIME AS dropoff_datetime,
    pu.Borough AS pickup_borough,
    pu.Zone AS pickup_zone,
    do.Borough AS dropoff_borough,
    do.Zone AS dropoff_zone,
    t.TRIP_DISTANCE,
    t.TOTAL_AMOUNT
FROM BRONZE.TRIP_DATA t
LEFT JOIN BRONZE.TAXI_ZONE_LOOKUP pu ON t.PULOCATIONID = pu.LocationID
LEFT JOIN BRONZE.TAXI_ZONE_LOOKUP do ON t.DOLOCATIONID = do.LocationID;

-- do some data quality checks if lookup table is connected

SELECT COUNT(*)
FROM BRONZE.TRIP_DATA t
LEFT JOIN BRONZE.TAXI_ZONE_LOOKUP pu ON t.PULOCATIONID = pu.LocationID
LEFT JOIN BRONZE.TAXI_ZONE_LOOKUP do ON t.DOLOCATIONID = do.LocationID
WHERE pu.locationid IS NULL OR do.locationID IS NULL
;

-- no issues, so we can use INNER JOIN or still use LEFT JOIN and check for NULL

------------------------------
-- CREATE & LOAD json files for weather data for NY
------------------------------

LIST @my_public_nyc_stage;

CREATE OR REPLACE TABLE BRONZE.WEATHER_REPORTS (
    JSON_DATA VARIANT,
    ADM_FILENAME VARCHAR,
    ADM_LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COPY INTO BRONZE.WEATHER_REPORTS (JSON_DATA, ADM_FILENAME)
FROM (
    SELECT 
        $1,
        METADATA$FILENAME
    FROM @MY_DATA_LAKE.BRONZE.MY_PUBLIC_NYC_STAGE/weather_reports/
)
FILE_FORMAT = (TYPE = 'JSON')
PATTERN = '.*\\.json';

SELECT *
FROM BRONZE.WEATHER_REPORTS
LIMIT 10;

-- Snowflake powerfull on-the-fly SQL like JSON parsing
SELECT 
    p.value:temp:minDateTimeISO::DATE AS min_datetime,
    p.value:temp:avgF::FLOAT AS temp_avg_f,
    p.value:temp:minF::FLOAT AS temp_min_f,
    p.value:temp:maxF::FLOAT AS temp_max_f
FROM BRONZE.WEATHER_REPORTS,
    LATERAL FLATTEN(input => JSON_DATA:response[0]:periods) p;


-- how to create simple user defined functions (UDFs)
CREATE OR REPLACE FUNCTION f_fahrenheit_to_celsius(temp_f FLOAT)
RETURNS FLOAT
AS
$$
    ROUND((temp_f - 32) * 5 / 9, 2)
$$;

SELECT 
    p.value:temp:minDateTimeISO::DATE AS date_measure,
    f_fahrenheit_to_celsius(p.value:temp:avgF::FLOAT) AS temp_avg_f,
    f_fahrenheit_to_celsius(p.value:temp:minF::FLOAT) AS temp_min_f,
    f_fahrenheit_to_celsius(p.value:temp:maxF::FLOAT) AS temp_max_f
FROM BRONZE.WEATHER_REPORTS,
    LATERAL FLATTEN(input => JSON_DATA:response[0]:periods) p;

