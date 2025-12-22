{{
    config(
        materialized='view',
        schema='intermediate'
    )
}}

WITH daily_weather AS (
    SELECT
        asset_id,
        date,
        
        -- Aggregated measurements
        ROUND(AVG(wind_speed_mps), 2) AS avg_wind_speed_mps,
        ROUND(MAX(wind_speed_mps), 2) AS max_wind_speed_mps,
        ROUND(MIN(wind_speed_mps), 2) AS min_wind_speed_mps,
        ROUND(STDDEV(wind_speed_mps), 2) AS std_wind_speed_mps,
        
        ROUND(AVG(ghi), 2) AS avg_ghi,
        ROUND(MAX(ghi), 2) AS max_ghi,
        ROUND(SUM(ghi), 2) AS total_ghi,
        
        ROUND(AVG(temperature_c), 2) AS avg_temperature_c,
        ROUND(MAX(temperature_c), 2) AS max_temperature_c,
        ROUND(MIN(temperature_c), 2) AS min_temperature_c,
        
        ROUND(AVG(pressure_hpa), 1) AS avg_pressure_hpa,
        ROUND(AVG(relative_humidity), 1) AS avg_relative_humidity,
        
        -- Count of observations
        COUNT(*) AS observation_count,
        COUNT(CASE WHEN is_data_complete THEN 1 END) AS complete_observation_count,
        
        -- Data quality
        ROUND(
            COUNT(CASE WHEN is_data_complete THEN 1 END)::DOUBLE / COUNT(*)::DOUBLE * 100,
            2
        ) AS data_completeness_pct,
        
        -- Peak hours statistics (solar: 10am-4pm)
        ROUND(
            AVG(CASE WHEN hour_of_day BETWEEN 10 AND 16 THEN ghi END),
            2
        ) AS avg_peak_ghi,
        
        -- Wind patterns
        COUNT(CASE WHEN wind_speed_category = 'Strong' THEN 1 END) AS strong_wind_hours,
        COUNT(CASE WHEN wind_speed_category = 'Very Strong' THEN 1 END) AS very_strong_wind_hours,
        
        -- Daylight hours (GHI > 0)
        COUNT(CASE WHEN ghi > 0 THEN 1 END) AS daylight_hours,
        
        MIN(timestamp) AS first_observation_timestamp,
        MAX(timestamp) AS last_observation_timestamp
        
    FROM {{ ref('stg_weather') }}
    GROUP BY asset_id, date
)

SELECT * FROM daily_weather
