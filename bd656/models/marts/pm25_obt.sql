-- {{
-- config(
--     materialized = 'materialized_view',
--     on_configuration_change = 'apply',
--     enable_refresh = True,
--     refresh_interval_minutes = 30,
--     max_staleness = 'INTERVAL 60 MINUTE'
-- )
-- }}

-- Combine data from multiple staging tables
WITH combined_data AS (
    SELECT *
    FROM {{ ref('stg__pcd') }} AS pcd

    UNION ALL

    SELECT *
    FROM {{ ref('stg__ccdc') }} AS ccdc
)

SELECT *
FROM combined_data
