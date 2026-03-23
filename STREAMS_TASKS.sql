--------------------------------------------------------------
-- STREAMS DEMO: BRONZE → SILVER CDC Pipeline
-- This script demonstrates Snowflake Streams for
-- Change Data Capture (CDC) between BRONZE and SILVER layers
--------------------------------------------------------------

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE XS_WH;
USE DATABASE GREGA_DATA_LAKE;

--------------------------------------------------------------
-- STEP 1: Create the source table in BRONZE
--------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.CUSTOMERS (
    CUSTOMER_ID   NUMBER       NOT NULL,
    FIRST_NAME    VARCHAR(100) NOT NULL,
    LAST_NAME     VARCHAR(100) NOT NULL,
    EMAIL         VARCHAR(255),
    CITY          VARCHAR(100),
    SIGNUP_DATE   DATE,
    IS_ACTIVE     BOOLEAN      DEFAULT TRUE,
    UPDATED_AT    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

--------------------------------------------------------------
-- STEP 2: Create the SILVER target table (same structure)
--------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.CUSTOMERS (
    CUSTOMER_ID   NUMBER       NOT NULL,
    FIRST_NAME    VARCHAR(100) NOT NULL,
    LAST_NAME     VARCHAR(100) NOT NULL,
    EMAIL         VARCHAR(255),
    CITY          VARCHAR(100),
    SIGNUP_DATE   DATE,
    IS_ACTIVE     BOOLEAN      DEFAULT TRUE,
    UPDATED_AT    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

--------------------------------------------------------------
-- STEP 3: Create a STREAM on the BRONZE table
--------------------------------------------------------------
CREATE OR REPLACE STREAM BRONZE.CUSTOMERS_STREAM
    ON TABLE BRONZE.CUSTOMERS;

--------------------------------------------------------------
-- STEP 4: Insert 10 rows into BRONZE
--------------------------------------------------------------
INSERT INTO BRONZE.CUSTOMERS
    (CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, CITY, SIGNUP_DATE, IS_ACTIVE)
VALUES
    (1,  'Alice',   'Johnson',  'alice@example.com',   'New York',      '2025-01-15', TRUE),
    (2,  'Bob',     'Smith',    'bob@example.com',     'Los Angeles',   '2025-02-20', TRUE),
    (3,  'Charlie', 'Brown',    'charlie@example.com', 'Chicago',       '2025-03-10', TRUE),
    (4,  'Diana',   'Prince',   'diana@example.com',   'Seattle',       '2025-04-05', TRUE),
    (5,  'Edward',  'Norton',   'edward@example.com',  'Boston',        '2025-05-18', TRUE),
    (6,  'Fiona',   'Apple',    'fiona@example.com',   'Austin',        '2025-06-22', TRUE),
    (7,  'George',  'Miller',   'george@example.com',  'Denver',        '2025-07-30', TRUE),
    (8,  'Hannah',  'Lee',      'hannah@example.com',  'Portland',      '2025-08-14', TRUE),
    (9,  'Ivan',    'Petrov',   'ivan@example.com',    'Miami',         '2025-09-01', FALSE),
    (10, 'Julia',   'Roberts',  'julia@example.com',   'San Francisco', '2025-10-10', TRUE);

--------------------------------------------------------------
-- STEP 5: Verify the stream captured the 10 INSERTs
--------------------------------------------------------------
SELECT * FROM BRONZE.CUSTOMERS_STREAM;

--------------------------------------------------------------
-- STEP 6: Consume the stream — MERGE into SILVER
-- This advances the stream offset
--------------------------------------------------------------
MERGE INTO SILVER.CUSTOMERS AS tgt
USING (
    SELECT
        CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL,
        CITY, SIGNUP_DATE, IS_ACTIVE, UPDATED_AT,
        METADATA$ACTION, METADATA$ISUPDATE
    FROM BRONZE.CUSTOMERS_STREAM
) AS src
ON tgt.CUSTOMER_ID = src.CUSTOMER_ID
WHEN MATCHED AND src.METADATA$ACTION = 'INSERT' AND src.METADATA$ISUPDATE = TRUE THEN
    UPDATE SET
        tgt.FIRST_NAME  = src.FIRST_NAME,
        tgt.LAST_NAME   = src.LAST_NAME,
        tgt.EMAIL        = src.EMAIL,
        tgt.CITY         = src.CITY,
        tgt.SIGNUP_DATE  = src.SIGNUP_DATE,
        tgt.IS_ACTIVE    = src.IS_ACTIVE,
        tgt.UPDATED_AT   = src.UPDATED_AT
WHEN MATCHED AND src.METADATA$ACTION = 'DELETE' AND src.METADATA$ISUPDATE = FALSE THEN
    DELETE
WHEN NOT MATCHED AND src.METADATA$ACTION = 'INSERT' THEN
    INSERT (CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, CITY, SIGNUP_DATE, IS_ACTIVE, UPDATED_AT)
    VALUES (src.CUSTOMER_ID, src.FIRST_NAME, src.LAST_NAME, src.EMAIL, src.CITY, src.SIGNUP_DATE, src.IS_ACTIVE, src.UPDATED_AT);

--------------------------------------------------------------
-- STEP 7: Verify SILVER now has all 10 rows
--------------------------------------------------------------
SELECT * FROM SILVER.CUSTOMERS ORDER BY CUSTOMER_ID;

--------------------------------------------------------------
-- STEP 8: Verify the stream is now empty (offset advanced)
--------------------------------------------------------------
SELECT * FROM BRONZE.CUSTOMERS_STREAM;

--------------------------------------------------------------
-- STEP 9: Make changes in BRONZE (UPDATE + DELETE + INSERT)
-- to demonstrate full CDC capabilities
--------------------------------------------------------------

UPDATE BRONZE.CUSTOMERS
SET EMAIL = 'alice.johnson@newmail.com', CITY = 'Brooklyn', UPDATED_AT = CURRENT_TIMESTAMP()
WHERE CUSTOMER_ID = 1;

DELETE FROM BRONZE.CUSTOMERS WHERE CUSTOMER_ID = 9;

INSERT INTO BRONZE.CUSTOMERS
    (CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, CITY, SIGNUP_DATE, IS_ACTIVE)
VALUES
    (11, 'Kevin', 'Hart', 'kevin@example.com', 'Philadelphia', '2026-01-05', TRUE);

--------------------------------------------------------------
-- STEP 10: Check stream - notice the logic for UPDATE (DELETE + INSERT)
--------------------------------------------------------------
SELECT * FROM BRONZE.CUSTOMERS_STREAM;

--------------------------------------------------------------
-- STEP 11: Consume again — MERGE changes into SILVER
--------------------------------------------------------------
MERGE INTO SILVER.CUSTOMERS AS tgt
USING (
    SELECT
        CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL,
        CITY, SIGNUP_DATE, IS_ACTIVE, UPDATED_AT,
        METADATA$ACTION, METADATA$ISUPDATE
    FROM BRONZE.CUSTOMERS_STREAM
) AS src
ON tgt.CUSTOMER_ID = src.CUSTOMER_ID
WHEN MATCHED AND src.METADATA$ACTION = 'INSERT' AND src.METADATA$ISUPDATE = TRUE THEN
    UPDATE SET
        tgt.FIRST_NAME  = src.FIRST_NAME,
        tgt.LAST_NAME   = src.LAST_NAME,
        tgt.EMAIL        = src.EMAIL,
        tgt.CITY         = src.CITY,
        tgt.SIGNUP_DATE  = src.SIGNUP_DATE,
        tgt.IS_ACTIVE    = src.IS_ACTIVE,
        tgt.UPDATED_AT   = src.UPDATED_AT
WHEN MATCHED AND src.METADATA$ACTION = 'DELETE' AND src.METADATA$ISUPDATE = FALSE THEN
    DELETE
WHEN NOT MATCHED AND src.METADATA$ACTION = 'INSERT' THEN
    INSERT (CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, CITY, SIGNUP_DATE, IS_ACTIVE, UPDATED_AT)
    VALUES (src.CUSTOMER_ID, src.FIRST_NAME, src.LAST_NAME, src.EMAIL, src.CITY, src.SIGNUP_DATE, src.IS_ACTIVE, src.UPDATED_AT);

--------------------------------------------------------------
-- STEP 12: Final verification — SILVER reflects all changes
--------------------------------------------------------------
SELECT * FROM SILVER.CUSTOMERS ORDER BY CUSTOMER_ID;

--------------------------------------------------------------
-- STEP 13: Create a TASK that auto-runs the MERGE
-- when the stream has data (triggered task)
--------------------------------------------------------------
CREATE OR REPLACE TASK BRONZE.CUSTOMERS_CDC_TASK
    WAREHOUSE = XS_WH
    WHEN SYSTEM$STREAM_HAS_DATA('BRONZE.CUSTOMERS_STREAM')
AS
MERGE INTO SILVER.CUSTOMERS AS tgt
USING (
    SELECT
        CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL,
        CITY, SIGNUP_DATE, IS_ACTIVE, UPDATED_AT,
        METADATA$ACTION, METADATA$ISUPDATE
    FROM BRONZE.CUSTOMERS_STREAM
) AS src
ON tgt.CUSTOMER_ID = src.CUSTOMER_ID
WHEN MATCHED AND src.METADATA$ACTION = 'INSERT' AND src.METADATA$ISUPDATE = TRUE THEN
    UPDATE SET
        tgt.FIRST_NAME  = src.FIRST_NAME,
        tgt.LAST_NAME   = src.LAST_NAME,
        tgt.EMAIL        = src.EMAIL,
        tgt.CITY         = src.CITY,
        tgt.SIGNUP_DATE  = src.SIGNUP_DATE,
        tgt.IS_ACTIVE    = src.IS_ACTIVE,
        tgt.UPDATED_AT   = src.UPDATED_AT
WHEN MATCHED AND src.METADATA$ACTION = 'DELETE' AND src.METADATA$ISUPDATE = FALSE THEN
    DELETE
WHEN NOT MATCHED AND src.METADATA$ACTION = 'INSERT' THEN
    INSERT (CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, CITY, SIGNUP_DATE, IS_ACTIVE, UPDATED_AT)
    VALUES (src.CUSTOMER_ID, src.FIRST_NAME, src.LAST_NAME, src.EMAIL, src.CITY, src.SIGNUP_DATE, src.IS_ACTIVE, src.UPDATED_AT);

--------------------------------------------------------------
-- STEP 14: Resume the task (tasks are created suspended)
--------------------------------------------------------------
ALTER TASK BRONZE.CUSTOMERS_CDC_TASK RESUME;

--------------------------------------------------------------
-- STEP 15: Test it — insert new data into BRONZE
-- The task will auto-trigger and MERGE into SILVER
--------------------------------------------------------------
INSERT INTO BRONZE.CUSTOMERS
    (CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, CITY, SIGNUP_DATE, IS_ACTIVE)
VALUES
    (12, 'Laura', 'Palmer', 'laura@example.com', 'Twin Peaks', '2026-03-22', TRUE);

--------------------------------------------------------------
-- STEP 16: Check task execution history
--------------------------------------------------------------
SELECT NAME, STATE, SCHEDULED_TIME, COMPLETED_TIME, SCHEDULED_FROM
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'CUSTOMERS_CDC_TASK',
    SCHEDULED_TIME_RANGE_START => DATEADD('HOUR', -1, CURRENT_TIMESTAMP())
))
ORDER BY SCHEDULED_TIME DESC;

SELECT * FROM SILVER.CUSTOMERS ORDER BY CUSTOMER_ID;

--------------------------------------------------------------
-- STEP 17: Suspend the task when done testing
--------------------------------------------------------------
ALTER TASK GREGA_DATA_LAKE.BRONZE.CUSTOMERS_CDC_TASK SUSPEND;
