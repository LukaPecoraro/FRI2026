WITH unknown_row AS (
    SELECT
        -1 AS rate_code_sk,
        -1 AS rate_code_id,
        'Unknown' AS rate_code_desc
),

rate_codes AS (
    SELECT
        rate_code_id AS rate_code_sk,
        rate_code_id,
        CASE rate_code_id
            WHEN 1 THEN 'Standard Rate'
            WHEN 2 THEN 'JFK'
            WHEN 3 THEN 'Newark'
            WHEN 4 THEN 'Nassau/Westchester'
            WHEN 5 THEN 'Negotiated Fare'
            WHEN 6 THEN 'Group Ride'
            WHEN 99 THEN 'Unknown'
            ELSE 'Other'
        END AS rate_code_desc
    FROM (
        SELECT DISTINCT rate_code_id
        FROM {{ source('silver', 'trip_data') }}
        WHERE rate_code_id IS NOT NULL
    )
)

SELECT * FROM unknown_row
UNION ALL
SELECT * FROM rate_codes
ORDER BY rate_code_sk
