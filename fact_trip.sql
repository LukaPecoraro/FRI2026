SELECT
    COALESCE(dd_pu.date_sk, -1) AS pickup_date_sk,
    COALESCE(dd_do.date_sk, -1) AS dropoff_date_sk,
    COALESCE(dt_pu.time_sk, -1) AS pickup_time_sk,
    COALESCE(dt_do.time_sk, -1) AS dropoff_time_sk,
    COALESCE(dl_pu.location_sk, -1) AS pickup_location_sk,
    COALESCE(dl_do.location_sk, -1) AS dropoff_location_sk,
    COALESCE(dv.vendor_sk, -1) AS vendor_sk,
    COALESCE(dr.rate_code_sk, -1) AS rate_code_sk,
    COALESCE(dp.payment_type_sk, -1) AS payment_type_sk,
    t.passenger_count,
    t.trip_distance,
    t.fare_amount,
    t.extra,
    t.mta_tax,
    t.tip_amount,
    t.tolls_amount,
    t.improvement_surcharge,
    t.congestion_surcharge,
    t.airport_fee,
    t.cbd_congestion_fee,
    t.total_amount,
    t.store_and_fwd_flag,
    DATEDIFF('MINUTE', t.pickup_datetime, t.dropoff_datetime) AS trip_duration_minutes
FROM {{ source('silver', 'trip_data') }} t
LEFT JOIN {{ ref('dim_date') }} dd_pu
    ON dd_pu.date_sk = TO_NUMBER(TO_CHAR(t.pickup_datetime::DATE, 'YYYYMMDD'))
LEFT JOIN {{ ref('dim_date') }} dd_do
    ON dd_do.date_sk = TO_NUMBER(TO_CHAR(t.dropoff_datetime::DATE, 'YYYYMMDD'))
LEFT JOIN {{ ref('dim_time') }} dt_pu
    ON dt_pu.time_sk = (HOUR(t.pickup_datetime) * 100) + MINUTE(t.pickup_datetime)
LEFT JOIN {{ ref('dim_time') }} dt_do
    ON dt_do.time_sk = (HOUR(t.dropoff_datetime) * 100) + MINUTE(t.dropoff_datetime)
LEFT JOIN {{ ref('dim_location') }} dl_pu
    ON dl_pu.location_id = t.pu_location_id
LEFT JOIN {{ ref('dim_location') }} dl_do
    ON dl_do.location_id = t.do_location_id
LEFT JOIN {{ ref('dim_vendor') }} dv
    ON dv.vendor_id = t.vendor_id
LEFT JOIN {{ ref('dim_rate_code') }} dr
    ON dr.rate_code_id = t.rate_code_id
LEFT JOIN {{ ref('dim_payment_type') }} dp
    ON dp.payment_type_id = t.payment_type
