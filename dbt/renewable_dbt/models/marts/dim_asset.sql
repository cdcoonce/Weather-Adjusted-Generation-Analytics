{{
    config(
        materialized='table',
        schema='marts'
    )
}}

-- Asset dimension: one row per asset with site name, technology, capacity,
-- size classification, coordinates, and grid region. Sourced from the
-- ``asset_dimension`` seed (generated from the fleet registry). Downstream
-- consumers (the dashboard export, BI tools) join this for human-readable
-- asset metadata.

WITH source AS (
    SELECT
        asset_id,
        asset_name,
        asset_type,
        asset_capacity_mw,
        latitude,
        longitude,
        region
    FROM (
        SELECT
            asset_id::VARCHAR AS asset_id,
            asset_name::VARCHAR AS asset_name,
            asset_type::VARCHAR AS asset_type,
            capacity_mw::FLOAT AS asset_capacity_mw,
            latitude::FLOAT AS latitude,
            longitude::FLOAT AS longitude,
            region::VARCHAR AS region
        FROM {{ ref('asset_dimension') }}
    )
)

SELECT
    asset_id,
    asset_name,
    asset_type,
    asset_capacity_mw,
    CASE
        WHEN asset_capacity_mw >= 75 THEN 'Large'
        WHEN asset_capacity_mw >= 50 THEN 'Medium'
        ELSE 'Small'
    END AS asset_size_category,
    latitude,
    longitude,
    region
FROM source
