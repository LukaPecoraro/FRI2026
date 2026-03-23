WITH unknown_row AS (
    SELECT
        -1 AS time_sk,
        '00:00:00'::TIME AS full_time,
        0 AS hour,
        0 AS minute,
        'Unknown' AS time_period,
        FALSE AS is_rush_hour
),

time_spine AS (
    SELECT
        (h.hour * 100) + m.minute AS time_sk,
        TIME_FROM_PARTS(h.hour, m.minute, 0) AS full_time,
        h.hour,
        m.minute,
        CASE
            WHEN h.hour BETWEEN 6 AND 11 THEN 'Morning'
            WHEN h.hour BETWEEN 12 AND 16 THEN 'Afternoon'
            WHEN h.hour BETWEEN 17 AND 20 THEN 'Evening'
            ELSE 'Night'
        END AS time_period,
        CASE
            WHEN h.hour BETWEEN 7 AND 9 OR h.hour BETWEEN 16 AND 19 THEN TRUE
            ELSE FALSE
        END AS is_rush_hour
    FROM (
        SELECT SEQ4() AS hour FROM TABLE(GENERATOR(ROWCOUNT => 24))
    ) h
    CROSS JOIN (
        SELECT SEQ4() AS minute FROM TABLE(GENERATOR(ROWCOUNT => 60))
    ) m
)

SELECT * FROM unknown_row
UNION ALL
SELECT * FROM time_spine
ORDER BY time_sk
