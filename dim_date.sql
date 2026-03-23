WITH unknown_row AS (
    SELECT
        -1 AS date_sk,
        '1900-01-01'::DATE AS full_date,
        0 AS year,
        0 AS quarter,
        0 AS month,
        'Unknown' AS month_name,
        0 AS week,
        0 AS day_of_month,
        0 AS day_of_week,
        'Unknown' AS day_name,
        FALSE AS is_weekend,
        FALSE AS is_holiday
),

date_spine AS (
    SELECT
        TO_NUMBER(TO_CHAR(full_date, 'YYYYMMDD')) AS date_sk,
        full_date,
        YEAR(full_date) AS year,
        QUARTER(full_date) AS quarter,
        MONTH(full_date) AS month,
        MONTHNAME(full_date) AS month_name,
        WEEKOFYEAR(full_date) AS week,
        DAY(full_date) AS day_of_month,
        DAYOFWEEKISO(full_date) AS day_of_week,
        DAYNAME(full_date) AS day_name,
        CASE WHEN DAYOFWEEKISO(full_date) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend,
        FALSE AS is_holiday
    FROM (
        SELECT DATEADD(DAY, SEQ4(), '2025-01-01'::DATE) AS full_date
        FROM TABLE(GENERATOR(ROWCOUNT => 365))
    )
)

SELECT * FROM unknown_row
UNION ALL
SELECT * FROM date_spine
