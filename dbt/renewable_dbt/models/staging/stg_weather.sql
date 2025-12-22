{{
    config(
        materialized='view',
        schema='staging'
    )
}}

WITH source_data AS (
    SELECT
        timestamp,
        asset_id,
        wind_speed_mps,
        wind_direction_deg,
        ghi,
        temperature_c,
        pressure_hpa,
        relative_humidity,
        _dlt_load_id
    FROM {{ source('renewable_raw', 'weather') }}
),

deduplicated_source AS (
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY asset_id, timestamp ORDER BY _dlt_load_id DESC) as rn
        FROM source_data
    ) WHERE rn = 1
),

transformed AS (
    SELECT
        -- Primary keys
        timestamp AS timestamp,
        asset_id::VARCHAR AS asset_id,
        
        -- Weather measurements
        ROUND(wind_speed_mps::DOUBLE, 2) AS wind_speed_mps,
        ROUND(wind_direction_deg::DOUBLE, 1) AS wind_direction_deg,
        ROUND(ghi::DOUBLE, 2) AS ghi,
        ROUND(temperature_c::DOUBLE, 2) AS temperature_c,
        ROUND(pressure_hpa::DOUBLE, 1) AS pressure_hpa,
        ROUND(relative_humidity::DOUBLE, 1) AS relative_humidity,
        
        -- Derived time features
        EXTRACT(HOUR FROM timestamp) AS hour_of_day,
        EXTRACT(DAY FROM timestamp) AS day_of_month,
        EXTRACT(DOW FROM timestamp) AS day_of_week,
        EXTRACT(MONTH FROM timestamp) AS month,
        EXTRACT(QUARTER FROM timestamp) AS quarter,
        EXTRACT(YEAR FROM timestamp) AS year,
        DATE_TRUNC('day', timestamp) AS date,
        
        -- Derived weather features
        CASE
            WHEN wind_speed_mps < 3 THEN 'Calm'
            WHEN wind_speed_mps < 12 THEN 'Moderate'
            WHEN wind_speed_mps < 25 THEN 'Strong'
            ELSE 'Very Strong'
        END AS wind_speed_category,
        
        CASE
            WHEN ghi < 100 THEN 'Night/Cloudy'
            WHEN ghi < 400 THEN 'Low Irradiance'
            WHEN ghi < 700 THEN 'Moderate Irradiance'
            ELSE 'High Irradiance'
        END AS irradiance_category,
        
        CASE
            WHEN temperature_c < 0 THEN 'Below Freezing'
            WHEN temperature_c < 15 THEN 'Cold'
            WHEN temperature_c < 25 THEN 'Moderate'
            ELSE 'Warm'
        END AS temperature_category,
        
        -- Data quality flags
        CASE
            WHEN wind_speed_mps IS NULL
                OR wind_direction_deg IS NULL
                OR ghi IS NULL
                OR temperature_c IS NULL
            THEN FALSE
            ELSE TRUE
        END AS is_data_complete
        
    FROM deduplicated_source
)

SELECT DISTINCT * FROM transformed
