WITH unknown_row AS (
    SELECT
        -1 AS payment_type_sk,
        -1 AS payment_type_id,
        'Unknown' AS payment_desc
),

payment_types AS (
    SELECT
        payment_type AS payment_type_sk,
        payment_type AS payment_type_id,
        CASE payment_type
            WHEN 0 THEN 'Voided Trip'
            WHEN 1 THEN 'Credit Card'
            WHEN 2 THEN 'Cash'
            WHEN 3 THEN 'No Charge'
            WHEN 4 THEN 'Dispute'
            WHEN 5 THEN 'Unknown'
            ELSE 'Other'
        END AS payment_desc
    FROM (
        SELECT DISTINCT payment_type
        FROM {{ source('silver', 'trip_data') }}
        WHERE payment_type IS NOT NULL
    )
)

SELECT * FROM unknown_row
UNION ALL
SELECT * FROM payment_types
ORDER BY payment_type_sk
