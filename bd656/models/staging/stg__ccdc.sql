select
    date_time as datetime,
    ingest_datetime,
    ingest_date,
    "CCDC" as owned_by,
    name_th,
    latitude,
    longitude,
    pm25_value,
    province_en
from {{ source('ccdc', 'dustboy_test') }}
where
    ingest_datetime BETWEEN DATETIME("2024-04-01")
    AND DATETIME_ADD("2024-04-01", INTERVAL 30 DAY)
-- limit 3