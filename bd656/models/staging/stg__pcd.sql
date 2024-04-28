select
    datetime,
    ingest_datetime,
    ingest_date,
    "PCD" as owned_by,
    name_th,
    latitude,
    longitude,
    aqilast_pm25_value as pm25_value,
    province_en
from {{ source('pcd', 'air4thai_test') }}
-- where
--     ingest_datetime BETWEEN DATETIME("2024-03-28")
--     AND DATETIME_ADD("2024-03-28", INTERVAL 10 DAY)
-- limit 3