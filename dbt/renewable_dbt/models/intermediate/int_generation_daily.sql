{{
    config(
        materialized='view',
        schema='intermediate'
    )
}}

WITH daily_generation AS (
    SELECT
        asset_id,
        date,
        
        -- Asset characteristics
        MAX(asset_capacity_mw) AS asset_capacity_mw,
        MAX(asset_size_category) AS asset_size_category,
        
        -- Generation metrics
        ROUND(SUM(gross_generation_mwh), 4) AS total_gross_generation_mwh,
        ROUND(SUM(net_generation_mwh), 4) AS total_net_generation_mwh,
        ROUND(SUM(curtailment_mwh), 4) AS total_curtailment_mwh,
        
        ROUND(AVG(gross_generation_mwh), 4) AS avg_hourly_gross_mwh,
        ROUND(AVG(net_generation_mwh), 4) AS avg_hourly_net_mwh,
        ROUND(MAX(net_generation_mwh), 4) AS peak_hourly_net_mwh,
        
        -- Capacity factor calculations
        {{ calculate_capacity_factor('SUM(net_generation_mwh)', 'MAX(asset_capacity_mw)', 24.0) }} AS daily_capacity_factor,
        
        ROUND(
            AVG(capacity_factor),
            4
        ) AS avg_hourly_capacity_factor,
        
        -- Availability and performance
        ROUND(AVG(availability_pct), 2) AS avg_availability_pct,
        ROUND(MIN(availability_pct), 2) AS min_availability_pct,
        
        -- Loss metrics
        ROUND(
            (SUM(gross_generation_mwh) - SUM(net_generation_mwh)) / NULLIF(SUM(gross_generation_mwh), 0) * 100,
            2
        ) AS daily_loss_percentage,
        
        ROUND(
            SUM(curtailment_mwh) / NULLIF(SUM(gross_generation_mwh), 0) * 100,
            2
        ) AS daily_curtailment_percentage,
        
        -- Operating hours
        COUNT(*) AS total_hours,
        COUNT(CASE WHEN net_generation_mwh > 0 THEN 1 END) AS generating_hours,
        COUNT(CASE WHEN capacity_factor > 0.5 THEN 1 END) AS high_output_hours,
        
        -- Performance categories
        COUNT(CASE WHEN performance_category = 'Excellent' THEN 1 END) AS excellent_hours,
        COUNT(CASE WHEN performance_category = 'Good' THEN 1 END) AS good_hours,
        COUNT(CASE WHEN performance_category = 'Fair' THEN 1 END) AS fair_hours,
        COUNT(CASE WHEN performance_category = 'Poor' THEN 1 END) AS poor_hours,
        
        -- Data quality
        COUNT(CASE WHEN is_data_valid THEN 1 END) AS valid_observation_count,
        COUNT(CASE WHEN is_zero_generation_anomaly THEN 1 END) AS zero_generation_anomaly_count,
        COUNT(CASE WHEN meets_availability_target THEN 1 END) AS hours_meeting_availability_target,
        
        ROUND(
            COUNT(CASE WHEN is_data_valid THEN 1 END)::DOUBLE / COUNT(*)::DOUBLE * 100,
            2
        ) AS data_validity_pct,
        
        MIN(timestamp) AS first_generation_timestamp,
        MAX(timestamp) AS last_generation_timestamp
        
    FROM {{ ref('stg_generation') }}
    GROUP BY asset_id, date
)

SELECT * FROM daily_generation
