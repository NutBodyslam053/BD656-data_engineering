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
