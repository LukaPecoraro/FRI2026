USE ROLE ACCOUNTADMIN;
USE WAREHOUSE XS_WH;
USE DATABASE GREGA_DATA_LAKE;

------------------------------------------------------------
-- SEQUENCE FOR SURROGATE KEYS
------------------------------------------------------------

CREATE OR REPLACE SEQUENCE GOLD.SEQ_DIM_SK START = 1 INCREMENT = 1;

------------------------------------------------------------
-- DIMENSION TABLES
------------------------------------------------------------

CREATE OR REPLACE TABLE GOLD.DIM_DATE (
    DATE_SK         INT          NOT NULL,
    FULL_DATE       DATE         NOT NULL,
    YEAR            INT          NOT NULL,
    QUARTER         INT          NOT NULL,
    MONTH           INT          NOT NULL,
    MONTH_NAME      VARCHAR(10)  NOT NULL,
    WEEK            INT          NOT NULL,
    DAY_OF_MONTH    INT          NOT NULL,
    DAY_OF_WEEK     INT          NOT NULL,
    DAY_NAME        VARCHAR(10)  NOT NULL,
    IS_WEEKEND      BOOLEAN      NOT NULL,
    IS_HOLIDAY      BOOLEAN      NOT NULL
);

CREATE OR REPLACE TABLE GOLD.DIM_TIME (
    TIME_SK         INT          NOT NULL,
    FULL_TIME       TIME         NOT NULL,
    HOUR            INT          NOT NULL,
    MINUTE          INT          NOT NULL,
    TIME_PERIOD     VARCHAR(10)  NOT NULL,
    IS_RUSH_HOUR    BOOLEAN      NOT NULL
);

CREATE OR REPLACE TABLE GOLD.DIM_LOCATION (
    LOCATION_SK     INT          NOT NULL,
    LOCATION_ID     INT          NOT NULL,
    BOROUGH         VARCHAR(100),
    ZONE            VARCHAR(100),
    SERVICE_ZONE    VARCHAR(100)
);

CREATE OR REPLACE TABLE GOLD.DIM_VENDOR (
    VENDOR_SK       INT          NOT NULL,
    VENDOR_ID       INT          NOT NULL,
    VENDOR_NAME     VARCHAR(50)  NOT NULL
);

CREATE OR REPLACE TABLE GOLD.DIM_RATE_CODE (
    RATE_CODE_SK    INT          NOT NULL,
    RATE_CODE_ID    INT          NOT NULL,
    RATE_CODE_DESC  VARCHAR(50)  NOT NULL
);

CREATE OR REPLACE TABLE GOLD.DIM_PAYMENT_TYPE (
    PAYMENT_TYPE_SK   INT          NOT NULL,
    PAYMENT_TYPE_ID   INT          NOT NULL,
    PAYMENT_DESC      VARCHAR(50)  NOT NULL
);

------------------------------------------------------------
-- FACT TABLE (no surrogate PK)
------------------------------------------------------------

CREATE OR REPLACE TABLE GOLD.FACT_TRIP (
    PICKUP_DATE_SK          INT,
    DROPOFF_DATE_SK         INT,
    PICKUP_TIME_SK          INT,
    DROPOFF_TIME_SK         INT,
    PICKUP_LOCATION_SK      INT,
    DROPOFF_LOCATION_SK     INT,
    VENDOR_SK               INT,
    RATE_CODE_SK            INT,
    PAYMENT_TYPE_SK         INT,
    PASSENGER_COUNT         INT,
    TRIP_DISTANCE           FLOAT,
    FARE_AMOUNT             FLOAT,
    EXTRA                   FLOAT,
    MTA_TAX                 FLOAT,
    TIP_AMOUNT              FLOAT,
    TOLLS_AMOUNT            FLOAT,
    IMPROVEMENT_SURCHARGE   FLOAT,
    CONGESTION_SURCHARGE    FLOAT,
    AIRPORT_FEE             FLOAT,
    CBD_CONGESTION_FEE      FLOAT,
    TOTAL_AMOUNT            FLOAT,
    STORE_AND_FWD_FLAG      VARCHAR(1),
    TRIP_DURATION_MINUTES   FLOAT
);

-- INIT DATE & TIME DIMENSIONS

INSERT INTO GOLD.DIM_DATE
SELECT -1, '1900-01-01'::DATE, 0, 0, 0, 'Unknown', 0, 0, 0, 'Unknown', FALSE, FALSE;


------------------------------------------------------------
-- POPULATE DIM_DATE (2025-01-01 to 2025-12-31)
------------------------------------------------------------

INSERT INTO GOLD.DIM_DATE
SELECT
    TO_NUMBER(TO_CHAR(d.FULL_DATE, 'YYYYMMDD'))  AS DATE_SK,
    d.FULL_DATE,
    YEAR(d.FULL_DATE)                             AS YEAR,
    QUARTER(d.FULL_DATE)                          AS QUARTER,
    MONTH(d.FULL_DATE)                            AS MONTH,
    MONTHNAME(d.FULL_DATE)                        AS MONTH_NAME,
    WEEKOFYEAR(d.FULL_DATE)                       AS WEEK,
    DAY(d.FULL_DATE)                              AS DAY_OF_MONTH,
    DAYOFWEEKISO(d.FULL_DATE)                     AS DAY_OF_WEEK,
    DAYNAME(d.FULL_DATE)                          AS DAY_NAME,
    CASE WHEN DAYOFWEEKISO(d.FULL_DATE) IN (6, 7) THEN TRUE ELSE FALSE END AS IS_WEEKEND,
    FALSE                                         AS IS_HOLIDAY
FROM (
    SELECT DATEADD(DAY, SEQ4(), '2025-01-01'::DATE) AS FULL_DATE
    FROM TABLE(GENERATOR(ROWCOUNT => 365))
) d;

------------------------------------------------------------
-- POPULATE DIM_TIME (every minute: 00:00 to 23:59)
------------------------------------------------------------

INSERT INTO GOLD.DIM_TIME
SELECT -1, '00:00:00'::TIME, 0, 0, 'Unknown', FALSE;

INSERT INTO GOLD.DIM_TIME
SELECT
    (h.HOUR * 100) + m.MINUTE                    AS TIME_SK,
    TIME_FROM_PARTS(h.HOUR, m.MINUTE, 0)         AS FULL_TIME,
    h.HOUR,
    m.MINUTE,
    CASE
        WHEN h.HOUR BETWEEN 6  AND 11 THEN 'Morning'
        WHEN h.HOUR BETWEEN 12 AND 16 THEN 'Afternoon'
        WHEN h.HOUR BETWEEN 17 AND 20 THEN 'Evening'
        ELSE 'Night'
    END                                           AS TIME_PERIOD,
    CASE
        WHEN h.HOUR BETWEEN 7 AND 9 OR h.HOUR BETWEEN 16 AND 19 THEN TRUE
        ELSE FALSE
    END                                           AS IS_RUSH_HOUR
FROM (
    SELECT SEQ4() AS HOUR FROM TABLE(GENERATOR(ROWCOUNT => 24))
) h
CROSS JOIN (
    SELECT SEQ4() AS MINUTE FROM TABLE(GENERATOR(ROWCOUNT => 60))
) m
ORDER BY TIME_SK;

------------------------------------------------------------
-- POPULATE DIM_LOCATION from SILVER.TAXI_ZONE_LOOKUP
------------------------------------------------------------

MERGE INTO GOLD.DIM_LOCATION tgt
USING (
    SELECT -1 AS LOCATION_ID, 'Unknown' AS BOROUGH, 'Unknown' AS ZONE, 'Unknown' AS SERVICE_ZONE
    UNION ALL
    SELECT LOCATION_ID, BOROUGH, ZONE, SERVICE_ZONE
    FROM SILVER.TAXI_ZONE_LOOKUP
) src
ON tgt.LOCATION_ID = src.LOCATION_ID
WHEN MATCHED THEN UPDATE SET
    tgt.BOROUGH      = src.BOROUGH,
    tgt.ZONE         = src.ZONE,
    tgt.SERVICE_ZONE = src.SERVICE_ZONE
WHEN NOT MATCHED THEN INSERT (LOCATION_SK, LOCATION_ID, BOROUGH, ZONE, SERVICE_ZONE)
VALUES (
    CASE WHEN src.LOCATION_ID = -1 THEN -1 ELSE GOLD.SEQ_DIM_SK.NEXTVAL END,
    src.LOCATION_ID, src.BOROUGH, src.ZONE, src.SERVICE_ZONE
);

------------------------------------------------------------
-- POPULATE DIM_VENDOR from SILVER.TRIP_DATA
------------------------------------------------------------

MERGE INTO GOLD.DIM_VENDOR tgt
USING (
    SELECT -1 AS VENDOR_ID, 'Unknown' AS VENDOR_NAME
    UNION ALL
    SELECT
        VENDOR_ID,
        CASE VENDOR_ID
            WHEN 1 THEN 'Creative Mobile Technologies'
            WHEN 2 THEN 'VeriFone Inc.'
            WHEN 6 THEN 'Myle Technologies'
            WHEN 7 THEN 'Helix Technologies'
            ELSE 'Unknown'
        END AS VENDOR_NAME
    FROM (SELECT DISTINCT VENDOR_ID FROM SILVER.TRIP_DATA WHERE VENDOR_ID IS NOT NULL)
) src
ON tgt.VENDOR_ID = src.VENDOR_ID
WHEN MATCHED THEN UPDATE SET
    tgt.VENDOR_NAME = src.VENDOR_NAME
WHEN NOT MATCHED THEN INSERT (VENDOR_SK, VENDOR_ID, VENDOR_NAME)
VALUES (
    CASE WHEN src.VENDOR_ID = -1 THEN -1 ELSE GOLD.SEQ_DIM_SK.NEXTVAL END,
    src.VENDOR_ID, src.VENDOR_NAME
);

------------------------------------------------------------
-- POPULATE DIM_RATE_CODE from SILVER.TRIP_DATA
------------------------------------------------------------

MERGE INTO GOLD.DIM_RATE_CODE tgt
USING (
    SELECT -1 AS RATE_CODE_ID, 'Unknown' AS RATE_CODE_DESC
    UNION ALL
    SELECT
        RATE_CODE_ID,
        CASE RATE_CODE_ID
            WHEN 1 THEN 'Standard Rate'
            WHEN 2 THEN 'JFK'
            WHEN 3 THEN 'Newark'
            WHEN 4 THEN 'Nassau/Westchester'
            WHEN 5 THEN 'Negotiated Fare'
            WHEN 6 THEN 'Group Ride'
            WHEN 99 THEN 'Unknown'
            ELSE 'Other'
        END AS RATE_CODE_DESC
    FROM (SELECT DISTINCT RATE_CODE_ID FROM SILVER.TRIP_DATA WHERE RATE_CODE_ID IS NOT NULL)
) src
ON tgt.RATE_CODE_ID = src.RATE_CODE_ID
WHEN MATCHED THEN UPDATE SET
    tgt.RATE_CODE_DESC = src.RATE_CODE_DESC
WHEN NOT MATCHED THEN INSERT (RATE_CODE_SK, RATE_CODE_ID, RATE_CODE_DESC)
VALUES (
    CASE WHEN src.RATE_CODE_ID = -1 THEN -1 ELSE GOLD.SEQ_DIM_SK.NEXTVAL END,
    src.RATE_CODE_ID, src.RATE_CODE_DESC
);

------------------------------------------------------------
-- POPULATE DIM_PAYMENT_TYPE from SILVER.TRIP_DATA
------------------------------------------------------------

MERGE INTO GOLD.DIM_PAYMENT_TYPE tgt
USING (
    SELECT -1 AS PAYMENT_TYPE, 'Unknown' AS PAYMENT_DESC
    UNION ALL
    SELECT
        PAYMENT_TYPE,
        CASE PAYMENT_TYPE
            WHEN 0 THEN 'Voided Trip'
            WHEN 1 THEN 'Credit Card'
            WHEN 2 THEN 'Cash'
            WHEN 3 THEN 'No Charge'
            WHEN 4 THEN 'Dispute'
            WHEN 5 THEN 'Unknown'
            ELSE 'Other'
        END AS PAYMENT_DESC
    FROM (SELECT DISTINCT PAYMENT_TYPE FROM SILVER.TRIP_DATA WHERE PAYMENT_TYPE IS NOT NULL)
) src
ON tgt.PAYMENT_TYPE_ID = src.PAYMENT_TYPE
WHEN MATCHED THEN UPDATE SET
    tgt.PAYMENT_DESC = src.PAYMENT_DESC
WHEN NOT MATCHED THEN INSERT (PAYMENT_TYPE_SK, PAYMENT_TYPE_ID, PAYMENT_DESC)
VALUES (
    CASE WHEN src.PAYMENT_TYPE = -1 THEN -1 ELSE GOLD.SEQ_DIM_SK.NEXTVAL END,
    src.PAYMENT_TYPE, src.PAYMENT_DESC
);

------------------------------------------------------------
-- POPULATE FACT_TRIP from SILVER.TRIP_DATA + all dimensions
------------------------------------------------------------

TRUNCATE TABLE GOLD.FACT_TRIP;

INSERT INTO GOLD.FACT_TRIP
SELECT
    COALESCE(dd_pu.DATE_SK, -1)                         AS PICKUP_DATE_SK,
    COALESCE(dd_do.DATE_SK, -1)                         AS DROPOFF_DATE_SK,
    COALESCE(dt_pu.TIME_SK, -1)                         AS PICKUP_TIME_SK,
    COALESCE(dt_do.TIME_SK, -1)                         AS DROPOFF_TIME_SK,
    COALESCE(dl_pu.LOCATION_SK, -1)                     AS PICKUP_LOCATION_SK,
    COALESCE(dl_do.LOCATION_SK, -1)                     AS DROPOFF_LOCATION_SK,
    COALESCE(dv.VENDOR_SK, -1)                          AS VENDOR_SK,
    COALESCE(dr.RATE_CODE_SK, -1)                       AS RATE_CODE_SK,
    COALESCE(dp.PAYMENT_TYPE_SK, -1)                    AS PAYMENT_TYPE_SK,
    t.PASSENGER_COUNT,
    t.TRIP_DISTANCE,
    t.FARE_AMOUNT,
    t.EXTRA,
    t.MTA_TAX,
    t.TIP_AMOUNT,
    t.TOLLS_AMOUNT,
    t.IMPROVEMENT_SURCHARGE,
    t.CONGESTION_SURCHARGE,
    t.AIRPORT_FEE,
    t.CBD_CONGESTION_FEE,
    t.TOTAL_AMOUNT,
    t.STORE_AND_FWD_FLAG,
    DATEDIFF('MINUTE', t.PICKUP_DATETIME, t.DROPOFF_DATETIME) AS TRIP_DURATION_MINUTES
FROM SILVER.TRIP_DATA t
LEFT JOIN GOLD.DIM_DATE dd_pu
    ON dd_pu.DATE_SK = TO_NUMBER(TO_CHAR(t.PICKUP_DATETIME::DATE, 'YYYYMMDD'))
LEFT JOIN GOLD.DIM_DATE dd_do
    ON dd_do.DATE_SK = TO_NUMBER(TO_CHAR(t.DROPOFF_DATETIME::DATE, 'YYYYMMDD'))
LEFT JOIN GOLD.DIM_TIME dt_pu
    ON dt_pu.TIME_SK = (HOUR(t.PICKUP_DATETIME) * 100) + MINUTE(t.PICKUP_DATETIME)
LEFT JOIN GOLD.DIM_TIME dt_do
    ON dt_do.TIME_SK = (HOUR(t.DROPOFF_DATETIME) * 100) + MINUTE(t.DROPOFF_DATETIME)
LEFT JOIN GOLD.DIM_LOCATION dl_pu
    ON dl_pu.LOCATION_ID = t.PU_LOCATION_ID
LEFT JOIN GOLD.DIM_LOCATION dl_do
    ON dl_do.LOCATION_ID = t.DO_LOCATION_ID
LEFT JOIN GOLD.DIM_VENDOR dv
    ON dv.VENDOR_ID = t.VENDOR_ID
LEFT JOIN GOLD.DIM_RATE_CODE dr
    ON dr.RATE_CODE_ID = t.RATE_CODE_ID
LEFT JOIN GOLD.DIM_PAYMENT_TYPE dp
    ON dp.PAYMENT_TYPE_ID = t.PAYMENT_TYPE;

------------------------------------------------------------
-- TASK GRAPH: POPULATE GOLD LAYER
------------------------------------------------------------

CREATE OR REPLACE TASK GOLD.TASK_GOLD_ROOT
    WAREHOUSE = XS_WH
    SCHEDULE  = '60 MINUTES'
    SUSPEND_TASK_AFTER_NUM_FAILURES = 3
AS
    SELECT 1;

------------------------------------------------------------
-- DIMENSION TASKS (run in parallel after root)
------------------------------------------------------------

CREATE OR REPLACE TASK GOLD.TASK_DIM_LOCATION
    WAREHOUSE = XS_WH
    AFTER GOLD.TASK_GOLD_ROOT
AS
    MERGE INTO GOLD.DIM_LOCATION tgt
    USING (
        SELECT -1 AS LOCATION_ID, 'Unknown' AS BOROUGH, 'Unknown' AS ZONE, 'Unknown' AS SERVICE_ZONE
        UNION ALL
        SELECT LOCATION_ID, BOROUGH, ZONE, SERVICE_ZONE
        FROM SILVER.TAXI_ZONE_LOOKUP
    ) src
    ON tgt.LOCATION_ID = src.LOCATION_ID
    WHEN MATCHED THEN UPDATE SET
        tgt.BOROUGH      = src.BOROUGH,
        tgt.ZONE         = src.ZONE,
        tgt.SERVICE_ZONE = src.SERVICE_ZONE
    WHEN NOT MATCHED THEN INSERT (LOCATION_SK, LOCATION_ID, BOROUGH, ZONE, SERVICE_ZONE)
    VALUES (
        CASE WHEN src.LOCATION_ID = -1 THEN -1 ELSE GOLD.SEQ_DIM_SK.NEXTVAL END,
        src.LOCATION_ID, src.BOROUGH, src.ZONE, src.SERVICE_ZONE
    );

CREATE OR REPLACE TASK GOLD.TASK_DIM_VENDOR
    WAREHOUSE = XS_WH
    AFTER GOLD.TASK_GOLD_ROOT
AS
    MERGE INTO GOLD.DIM_VENDOR tgt
    USING (
        SELECT -1 AS VENDOR_ID, 'Unknown' AS VENDOR_NAME
        UNION ALL
        SELECT
            VENDOR_ID,
            CASE VENDOR_ID
                WHEN 1 THEN 'Creative Mobile Technologies'
                WHEN 2 THEN 'VeriFone Inc.'
                WHEN 6 THEN 'Myle Technologies'
                WHEN 7 THEN 'Helix Technologies'
                ELSE 'Unknown'
            END AS VENDOR_NAME
        FROM (SELECT DISTINCT VENDOR_ID FROM SILVER.TRIP_DATA WHERE VENDOR_ID IS NOT NULL)
    ) src
    ON tgt.VENDOR_ID = src.VENDOR_ID
    WHEN MATCHED THEN UPDATE SET
        tgt.VENDOR_NAME = src.VENDOR_NAME
    WHEN NOT MATCHED THEN INSERT (VENDOR_SK, VENDOR_ID, VENDOR_NAME)
    VALUES (
        CASE WHEN src.VENDOR_ID = -1 THEN -1 ELSE GOLD.SEQ_DIM_SK.NEXTVAL END,
        src.VENDOR_ID, src.VENDOR_NAME
    );

CREATE OR REPLACE TASK GOLD.TASK_DIM_RATE_CODE
    WAREHOUSE = XS_WH
    AFTER GOLD.TASK_GOLD_ROOT
AS
    MERGE INTO GOLD.DIM_RATE_CODE tgt
    USING (
        SELECT -1 AS RATE_CODE_ID, 'Unknown' AS RATE_CODE_DESC
        UNION ALL
        SELECT
            RATE_CODE_ID,
            CASE RATE_CODE_ID
                WHEN 1 THEN 'Standard Rate'
                WHEN 2 THEN 'JFK'
                WHEN 3 THEN 'Newark'
                WHEN 4 THEN 'Nassau/Westchester'
                WHEN 5 THEN 'Negotiated Fare'
                WHEN 6 THEN 'Group Ride'
                WHEN 99 THEN 'Unknown'
                ELSE 'Other'
            END AS RATE_CODE_DESC
        FROM (SELECT DISTINCT RATE_CODE_ID FROM SILVER.TRIP_DATA WHERE RATE_CODE_ID IS NOT NULL)
    ) src
    ON tgt.RATE_CODE_ID = src.RATE_CODE_ID
    WHEN MATCHED THEN UPDATE SET
        tgt.RATE_CODE_DESC = src.RATE_CODE_DESC
    WHEN NOT MATCHED THEN INSERT (RATE_CODE_SK, RATE_CODE_ID, RATE_CODE_DESC)
    VALUES (
        CASE WHEN src.RATE_CODE_ID = -1 THEN -1 ELSE GOLD.SEQ_DIM_SK.NEXTVAL END,
        src.RATE_CODE_ID, src.RATE_CODE_DESC
    );

CREATE OR REPLACE TASK GOLD.TASK_DIM_PAYMENT_TYPE
    WAREHOUSE = XS_WH
    AFTER GOLD.TASK_GOLD_ROOT
AS
    MERGE INTO GOLD.DIM_PAYMENT_TYPE tgt
    USING (
        SELECT -1 AS PAYMENT_TYPE, 'Unknown' AS PAYMENT_DESC
        UNION ALL
        SELECT
            PAYMENT_TYPE,
            CASE PAYMENT_TYPE
                WHEN 0 THEN 'Voided Trip'
                WHEN 1 THEN 'Credit Card'
                WHEN 2 THEN 'Cash'
                WHEN 3 THEN 'No Charge'
                WHEN 4 THEN 'Dispute'
                WHEN 5 THEN 'Unknown'
                ELSE 'Other'
            END AS PAYMENT_DESC
        FROM (SELECT DISTINCT PAYMENT_TYPE FROM SILVER.TRIP_DATA WHERE PAYMENT_TYPE IS NOT NULL)
    ) src
    ON tgt.PAYMENT_TYPE_ID = src.PAYMENT_TYPE
    WHEN MATCHED THEN UPDATE SET
        tgt.PAYMENT_DESC = src.PAYMENT_DESC
    WHEN NOT MATCHED THEN INSERT (PAYMENT_TYPE_SK, PAYMENT_TYPE_ID, PAYMENT_DESC)
    VALUES (
        CASE WHEN src.PAYMENT_TYPE = -1 THEN -1 ELSE GOLD.SEQ_DIM_SK.NEXTVAL END,
        src.PAYMENT_TYPE, src.PAYMENT_DESC
    );

------------------------------------------------------------
-- FACT TASK (runs after ALL dimension tasks complete)
------------------------------------------------------------

CREATE OR REPLACE TASK GOLD.TASK_FACT_TRIP
    WAREHOUSE = XS_WH
    AFTER GOLD.TASK_DIM_LOCATION,
          GOLD.TASK_DIM_VENDOR,
          GOLD.TASK_DIM_RATE_CODE,
          GOLD.TASK_DIM_PAYMENT_TYPE
AS
BEGIN
    TRUNCATE TABLE GOLD.FACT_TRIP;

    INSERT INTO GOLD.FACT_TRIP
    SELECT
        COALESCE(dd_pu.DATE_SK, -1),
        COALESCE(dd_do.DATE_SK, -1),
        COALESCE(dt_pu.TIME_SK, -1),
        COALESCE(dt_do.TIME_SK, -1),
        COALESCE(dl_pu.LOCATION_SK, -1),
        COALESCE(dl_do.LOCATION_SK, -1),
        COALESCE(dv.VENDOR_SK, -1),
        COALESCE(dr.RATE_CODE_SK, -1),
        COALESCE(dp.PAYMENT_TYPE_SK, -1),
        t.PASSENGER_COUNT,
        t.TRIP_DISTANCE,
        t.FARE_AMOUNT,
        t.EXTRA,
        t.MTA_TAX,
        t.TIP_AMOUNT,
        t.TOLLS_AMOUNT,
        t.IMPROVEMENT_SURCHARGE,
        t.CONGESTION_SURCHARGE,
        t.AIRPORT_FEE,
        t.CBD_CONGESTION_FEE,
        t.TOTAL_AMOUNT,
        t.STORE_AND_FWD_FLAG,
        DATEDIFF('MINUTE', t.PICKUP_DATETIME, t.DROPOFF_DATETIME)
    FROM SILVER.TRIP_DATA t
    LEFT JOIN GOLD.DIM_DATE dd_pu
        ON dd_pu.DATE_SK = TO_NUMBER(TO_CHAR(t.PICKUP_DATETIME::DATE, 'YYYYMMDD'))
    LEFT JOIN GOLD.DIM_DATE dd_do
        ON dd_do.DATE_SK = TO_NUMBER(TO_CHAR(t.DROPOFF_DATETIME::DATE, 'YYYYMMDD'))
    LEFT JOIN GOLD.DIM_TIME dt_pu
        ON dt_pu.TIME_SK = (HOUR(t.PICKUP_DATETIME) * 100) + MINUTE(t.PICKUP_DATETIME)
    LEFT JOIN GOLD.DIM_TIME dt_do
        ON dt_do.TIME_SK = (HOUR(t.DROPOFF_DATETIME) * 100) + MINUTE(t.DROPOFF_DATETIME)
    LEFT JOIN GOLD.DIM_LOCATION dl_pu
        ON dl_pu.LOCATION_ID = t.PU_LOCATION_ID
    LEFT JOIN GOLD.DIM_LOCATION dl_do
        ON dl_do.LOCATION_ID = t.DO_LOCATION_ID
    LEFT JOIN GOLD.DIM_VENDOR dv
        ON dv.VENDOR_ID = t.VENDOR_ID
    LEFT JOIN GOLD.DIM_RATE_CODE dr
        ON dr.RATE_CODE_ID = t.RATE_CODE_ID
    LEFT JOIN GOLD.DIM_PAYMENT_TYPE dp
        ON dp.PAYMENT_TYPE_ID = t.PAYMENT_TYPE;
END;

------------------------------------------------------------
-- RESUME ALL TASKS (children first, then root)
------------------------------------------------------------

ALTER TASK GOLD.TASK_FACT_TRIP RESUME;
ALTER TASK GOLD.TASK_DIM_LOCATION RESUME;
ALTER TASK GOLD.TASK_DIM_VENDOR RESUME;
ALTER TASK GOLD.TASK_DIM_RATE_CODE RESUME;
ALTER TASK GOLD.TASK_DIM_PAYMENT_TYPE RESUME;
ALTER TASK GOLD.TASK_GOLD_ROOT RESUME;

-- manually trigger
EXECUTE TASK GOLD.TASK_GOLD_ROOT;

ALTER TASK GOLD.TASK_FACT_TRIP SUSPEND;
ALTER TASK GOLD.TASK_DIM_LOCATION SUSPEND;
ALTER TASK GOLD.TASK_DIM_VENDOR SUSPEND;
ALTER TASK GOLD.TASK_DIM_RATE_CODE SUSPEND;
ALTER TASK GOLD.TASK_DIM_PAYMENT_TYPE SUSPEND;
ALTER TASK GOLD.TASK_GOLD_ROOT SUSPEND;
