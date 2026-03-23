WITH unknown_row AS (
    SELECT
        -1 AS vendor_sk,
        -1 AS vendor_id,
        'Unknown' AS vendor_name
),

vendors AS (
    SELECT
        vendor_id AS vendor_sk,
        vendor_id,
        CASE vendor_id
            WHEN 1 THEN 'Creative Mobile Technologies'
            WHEN 2 THEN 'VeriFone Inc.'
            WHEN 6 THEN 'Myle Technologies'
            WHEN 7 THEN 'Helix Technologies'
            ELSE 'Unknown'
        END AS vendor_name
    FROM (
        SELECT DISTINCT vendor_id
        FROM {{ source('silver', 'trip_data') }}
        WHERE vendor_id IS NOT NULL
    )
)

SELECT * FROM unknown_row
UNION ALL
SELECT * FROM vendors
ORDER BY vendor_sk
