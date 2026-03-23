WITH unknown_row AS (
    SELECT
        -1 AS location_sk,
        -1 AS location_id,
        'Unknown' AS borough,
        'Unknown' AS zone,
        'Unknown' AS service_zone
),

locations AS (
    SELECT
        location_id AS location_sk,
        location_id,
        borough,
        zone,
        service_zone
    FROM {{ source('silver', 'taxi_zone_lookup') }}
)

SELECT * FROM unknown_row
UNION ALL
SELECT * FROM locations
ORDER BY location_sk
