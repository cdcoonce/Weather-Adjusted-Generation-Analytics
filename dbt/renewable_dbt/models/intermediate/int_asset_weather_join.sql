{{
    config(
        materialized='view',
        schema='intermediate'
    )
}}

WITH weather_data AS (
    SELECT
        timestamp,
        asset_id,
        wind_speed_mps,
        ghi,
        temperature_c,
        pressure_hpa,
        is_data_complete
    FROM {{ ref('stg_weather') }}
),

generation_data AS (
    SELECT
        timestamp,
        asset_id,
        gross_generation_mwh,
        net_generation_mwh,
        curtailment_mwh,
        availability_pct,
        asset_capacity_mw,
        capacity_factor,
        is_data_valid
    FROM {{ ref('stg_generation') }}
),

joined AS (
    SELECT
        g.timestamp,
        g.asset_id,
        
        -- Generation metrics
        g.gross_generation_mwh,
        g.net_generation_mwh,
        g.curtailment_mwh,
        g.availability_pct,
        g.asset_capacity_mw,
        g.capacity_factor,
        
        -- Weather conditions
        w.wind_speed_mps,
        w.ghi,
        w.temperature_c,
        w.pressure_hpa,
        
        -- Data quality
        g.is_data_valid AS generation_data_valid,
        w.is_data_complete AS weather_data_complete,
        CASE
            WHEN g.is_data_valid AND w.is_data_complete THEN TRUE
            ELSE FALSE
        END AS is_complete_record,
        
        -- Time features
        EXTRACT(HOUR FROM g.timestamp) AS hour_of_day,
        EXTRACT(DOW FROM g.timestamp) AS day_of_week,
        DATE_TRUNC('day', g.timestamp) AS date
        
    FROM generation_data g
    INNER JOIN weather_data w
        ON g.asset_id = w.asset_id
        AND g.timestamp = w.timestamp
)

SELECT * FROM joined
