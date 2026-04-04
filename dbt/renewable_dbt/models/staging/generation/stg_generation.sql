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
        gross_generation_mwh,
        net_generation_mwh,
        curtailment_mwh,
        availability_pct,
        asset_capacity_mw,
        _dlt_load_id
    FROM {{ source('waga_raw', 'generation') }}
),

deduplicated_source AS (
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY asset_id, timestamp ORDER BY _dlt_load_id DESC) AS rn
        FROM source_data
    ) WHERE rn = 1
),

transformed AS (
    SELECT
        -- Primary keys
        timestamp AS timestamp,
        asset_id::VARCHAR AS asset_id,

        -- Generation measurements
        ROUND(gross_generation_mwh::FLOAT, 4) AS gross_generation_mwh,
        ROUND(net_generation_mwh::FLOAT, 4) AS net_generation_mwh,
        ROUND(curtailment_mwh::FLOAT, 4) AS curtailment_mwh,
        ROUND(availability_pct::FLOAT, 2) AS availability_pct,
        ROUND(asset_capacity_mw::FLOAT, 2) AS asset_capacity_mw,

        -- Derived time features
        EXTRACT(HOUR FROM timestamp) AS hour_of_day,
        EXTRACT(DAY FROM timestamp) AS day_of_month,
        DAYOFWEEK(timestamp) AS day_of_week,
        EXTRACT(MONTH FROM timestamp) AS month,
        EXTRACT(QUARTER FROM timestamp) AS quarter,
        EXTRACT(YEAR FROM timestamp) AS year,
        DATE_TRUNC('day', timestamp) AS date,

        -- Derived generation metrics
        {{ calculate_capacity_factor('net_generation_mwh', 'asset_capacity_mw', 1.0) }} AS capacity_factor,

        ROUND(
            (gross_generation_mwh - net_generation_mwh) / NULLIF(gross_generation_mwh, 0) * 100,
            2
        ) AS loss_percentage,

        ROUND(
            curtailment_mwh / NULLIF(gross_generation_mwh, 0) * 100,
            2
        ) AS curtailment_percentage,

        ROUND(
            net_generation_mwh / NULLIF(asset_capacity_mw, 0),
            4
        ) AS hours_at_capacity,

        -- Asset classification
        CASE
            WHEN asset_capacity_mw >= 75 THEN 'Large'
            WHEN asset_capacity_mw >= 50 THEN 'Medium'
            ELSE 'Small'
        END AS asset_size_category,

        -- Performance flags (inline capacity_factor to avoid alias reference in same SELECT)
        CASE
            WHEN {{ calculate_capacity_factor('net_generation_mwh', 'asset_capacity_mw', 1.0) }} > 0.9 THEN 'Excellent'
            WHEN {{ calculate_capacity_factor('net_generation_mwh', 'asset_capacity_mw', 1.0) }} > 0.6 THEN 'Good'
            WHEN {{ calculate_capacity_factor('net_generation_mwh', 'asset_capacity_mw', 1.0) }} > 0.3 THEN 'Fair'
            WHEN {{ calculate_capacity_factor('net_generation_mwh', 'asset_capacity_mw', 1.0) }} > 0.05 THEN 'Poor'
            ELSE 'Very Poor'
        END AS performance_category,

        CASE
            WHEN availability_pct >= {{ var('min_availability_pct') }} THEN TRUE
            ELSE FALSE
        END AS meets_availability_target,

        -- Data quality flags
        CASE
            WHEN gross_generation_mwh IS NULL
                OR net_generation_mwh IS NULL
                OR asset_capacity_mw IS NULL
                OR net_generation_mwh > gross_generation_mwh
                OR net_generation_mwh > asset_capacity_mw * 1.1  -- Allow 10% overgen
            THEN FALSE
            ELSE TRUE
        END AS is_data_valid,

        CASE
            WHEN net_generation_mwh = 0 AND availability_pct > 0 THEN TRUE
            ELSE FALSE
        END AS is_zero_generation_anomaly

    FROM deduplicated_source
)

SELECT * FROM transformed
