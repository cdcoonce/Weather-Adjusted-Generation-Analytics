{{
    config(
        materialized='table',
        schema='marts'
    )
}}

WITH daily_generation AS (
    SELECT * FROM {{ ref('int_generation_daily') }}
),

daily_weather AS (
    SELECT * FROM {{ ref('int_weather_daily') }}
),

asset_daily_performance AS (
    SELECT
        g.asset_id,
        g.date,
        
        -- Asset info
        g.asset_capacity_mw,
        g.asset_size_category,
        
        -- Generation metrics
        g.total_gross_generation_mwh,
        g.total_net_generation_mwh,
        g.total_curtailment_mwh,
        g.avg_hourly_net_mwh,
        g.peak_hourly_net_mwh,
        
        -- Capacity factor
        g.daily_capacity_factor,
        g.avg_hourly_capacity_factor,
        
        -- Availability
        g.avg_availability_pct,
        g.min_availability_pct,
        
        -- Loss metrics
        g.daily_loss_percentage,
        g.daily_curtailment_percentage,
        
        -- Operating hours
        g.total_hours,
        g.generating_hours,
        g.high_output_hours,
        ROUND(g.generating_hours::DOUBLE / g.total_hours::DOUBLE * 100, 2) AS generating_hours_pct,
        
        -- Performance distribution
        g.excellent_hours,
        g.good_hours,
        g.fair_hours,
        g.poor_hours,
        
        -- Weather conditions
        w.avg_wind_speed_mps,
        w.max_wind_speed_mps,
        w.avg_ghi,
        w.max_ghi,
        w.total_ghi,
        w.avg_temperature_c,
        w.strong_wind_hours,
        w.daylight_hours,
        
        -- Data quality
        g.data_validity_pct,
        w.data_completeness_pct,
        g.zero_generation_anomaly_count,
        
        -- Performance flags
        CASE
            WHEN g.daily_capacity_factor >= 0.6 THEN 'High'
            WHEN g.daily_capacity_factor >= 0.3 THEN 'Medium'
            ELSE 'Low'
        END AS daily_performance_rating,
        
        CASE
            WHEN g.avg_availability_pct >= {{ var('min_availability_pct') }} THEN TRUE
            ELSE FALSE
        END AS meets_availability_target,
        
        CASE
            WHEN g.data_validity_pct >= 95 AND w.data_completeness_pct >= 95 THEN TRUE
            ELSE FALSE
        END AS has_high_quality_data,
        
        -- Timestamps
        g.first_generation_timestamp,
        g.last_generation_timestamp
        
    FROM daily_generation g
    LEFT JOIN daily_weather w
        ON g.asset_id = w.asset_id
        AND g.date = w.date
)

SELECT * FROM asset_daily_performance
