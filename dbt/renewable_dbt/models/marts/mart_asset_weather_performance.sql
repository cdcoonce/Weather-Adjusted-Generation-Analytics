{{
    config(
        materialized='table',
        schema='marts'
    )
}}

WITH hourly_data AS (
    SELECT
        asset_id,
        date,
        wind_speed_mps,
        ghi,
        net_generation_mwh,
        capacity_factor,
        asset_capacity_mw
    FROM {{ ref('int_asset_weather_join') }}
    WHERE is_complete_record = TRUE
),

-- Calculate rolling correlations for each asset
asset_correlations AS (
    SELECT
        asset_id,
        
        -- Asset characteristics
        MAX(asset_capacity_mw) AS asset_capacity_mw,
        
        -- Overall statistics
        COUNT(*) AS total_observations,
        ROUND(AVG(net_generation_mwh), 4) AS avg_net_generation_mwh,
        ROUND(AVG(capacity_factor), 4) AS avg_capacity_factor,
        ROUND(AVG(wind_speed_mps), 2) AS avg_wind_speed_mps,
        ROUND(AVG(ghi), 2) AS avg_ghi,
        
        -- Correlation calculations
        ROUND(CORR(wind_speed_mps, net_generation_mwh), 4) AS wind_generation_correlation,
        ROUND(CORR(ghi, net_generation_mwh), 4) AS solar_generation_correlation,
        ROUND(CORR(wind_speed_mps, capacity_factor), 4) AS wind_cf_correlation,
        ROUND(CORR(ghi, capacity_factor), 4) AS solar_cf_correlation,
        
        -- Regression coefficients (simple linear model)
        ROUND(REGR_SLOPE(net_generation_mwh, wind_speed_mps), 6) AS wind_regression_slope,
        ROUND(REGR_INTERCEPT(net_generation_mwh, wind_speed_mps), 6) AS wind_regression_intercept,
        ROUND(REGR_SLOPE(net_generation_mwh, ghi), 6) AS solar_regression_slope,
        ROUND(REGR_INTERCEPT(net_generation_mwh, ghi), 6) AS solar_regression_intercept,
        
        -- R-squared approximation
        ROUND(POWER(CORR(wind_speed_mps, net_generation_mwh), 2), 4) AS wind_r_squared,
        ROUND(POWER(CORR(ghi, net_generation_mwh), 2), 4) AS solar_r_squared
        
    FROM hourly_data
    GROUP BY asset_id
),

-- Calculate daily statistics for trending
daily_stats AS (
    SELECT
        asset_id,
        date,
        
        COUNT(*) AS hourly_observations,
        ROUND(AVG(net_generation_mwh), 4) AS daily_avg_net_generation_mwh,
        ROUND(SUM(net_generation_mwh), 4) AS daily_total_net_generation_mwh,
        ROUND(AVG(capacity_factor), 4) AS daily_avg_capacity_factor,
        ROUND(AVG(wind_speed_mps), 2) AS daily_avg_wind_speed_mps,
        ROUND(AVG(ghi), 2) AS daily_avg_ghi
        
    FROM hourly_data
    GROUP BY asset_id, date
),

-- Calculate 7-day and 30-day rolling correlations
rolling_correlations AS (
    SELECT
        asset_id,
        date,
        
        -- 7-day rolling averages
        ROUND(
            AVG(daily_avg_net_generation_mwh) OVER (
                PARTITION BY asset_id
                ORDER BY date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ),
            4
        ) AS rolling_7d_avg_generation,
        
        ROUND(
            AVG(daily_avg_capacity_factor) OVER (
                PARTITION BY asset_id
                ORDER BY date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ),
            4
        ) AS rolling_7d_avg_cf,
        
        -- 30-day rolling averages
        ROUND(
            AVG(daily_avg_net_generation_mwh) OVER (
                PARTITION BY asset_id
                ORDER BY date
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ),
            4
        ) AS rolling_30d_avg_generation,
        
        ROUND(
            AVG(daily_avg_capacity_factor) OVER (
                PARTITION BY asset_id
                ORDER BY date
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ),
            4
        ) AS rolling_30d_avg_cf
        
    FROM daily_stats
),

-- Calculate expected generation using regression models
expected_generation AS (
    SELECT
        h.asset_id,
        h.date,
        h.wind_speed_mps,
        h.ghi,
        h.net_generation_mwh AS actual_generation_mwh,
        h.capacity_factor AS actual_capacity_factor,
        
        -- Expected generation from wind
        ROUND(
            GREATEST(
                c.wind_regression_intercept + (c.wind_regression_slope * h.wind_speed_mps),
                0
            ),
            4
        ) AS expected_generation_from_wind,
        
        -- Expected generation from solar
        ROUND(
            GREATEST(
                c.solar_regression_intercept + (c.solar_regression_slope * h.ghi),
                0
            ),
            4
        ) AS expected_generation_from_solar,
        
        c.wind_generation_correlation,
        c.solar_generation_correlation,
        c.wind_r_squared,
        c.solar_r_squared
        
    FROM hourly_data h
    INNER JOIN asset_correlations c
        ON h.asset_id = c.asset_id
),

-- Calculate performance scores
performance_scores AS (
    SELECT
        asset_id,
        date,
        
        -- Choose expected generation based on stronger correlation
        CASE
            WHEN ABS(wind_generation_correlation) > ABS(solar_generation_correlation)
            THEN expected_generation_from_wind
            ELSE expected_generation_from_solar
        END AS expected_generation_mwh,
        
        actual_generation_mwh,
        
        -- Performance ratio
        ROUND(
            actual_generation_mwh / NULLIF(
                CASE
                    WHEN ABS(wind_generation_correlation) > ABS(solar_generation_correlation)
                    THEN expected_generation_from_wind
                    ELSE expected_generation_from_solar
                END,
                0
            ) * 100,
            2
        ) AS performance_ratio_pct,
        
        wind_generation_correlation,
        solar_generation_correlation
        
    FROM expected_generation
),

-- Aggregate to daily level with scores
daily_performance_scores AS (
    SELECT
        asset_id,
        date,
        
        ROUND(AVG(expected_generation_mwh), 4) AS avg_expected_generation_mwh,
        ROUND(AVG(actual_generation_mwh), 4) AS avg_actual_generation_mwh,
        ROUND(AVG(performance_ratio_pct), 2) AS avg_performance_ratio_pct,
        
        -- Performance score (0-100)
        ROUND(
            LEAST(
                GREATEST(AVG(performance_ratio_pct), 0),
                100
            ),
            2
        ) AS performance_score,
        
        CASE
            WHEN AVG(performance_ratio_pct) >= 95 THEN 'Excellent'
            WHEN AVG(performance_ratio_pct) >= 85 THEN 'Good'
            WHEN AVG(performance_ratio_pct) >= 70 THEN 'Fair'
            ELSE 'Poor'
        END AS performance_category
        
    FROM performance_scores
    GROUP BY asset_id, date
),

-- Final mart combining all metrics
final_mart AS (
    SELECT
        ac.asset_id,
        ps.date,
        
        -- Asset characteristics
        ac.asset_capacity_mw,
        
        -- Overall correlations
        ac.wind_generation_correlation,
        ac.solar_generation_correlation,
        ac.wind_cf_correlation,
        ac.solar_cf_correlation,
        ac.wind_r_squared,
        ac.solar_r_squared,
        
        -- Regression parameters
        ac.wind_regression_slope,
        ac.wind_regression_intercept,
        ac.solar_regression_slope,
        ac.solar_regression_intercept,
        
        -- Daily performance
        ps.avg_expected_generation_mwh,
        ps.avg_actual_generation_mwh,
        ps.avg_performance_ratio_pct,
        ps.performance_score,
        ps.performance_category,
        
        -- Rolling metrics
        rc.rolling_7d_avg_generation,
        rc.rolling_7d_avg_cf,
        rc.rolling_30d_avg_generation,
        rc.rolling_30d_avg_cf,
        
        -- Asset type inference (based on stronger correlation)
        CASE
            WHEN ABS(ac.wind_generation_correlation) > ABS(ac.solar_generation_correlation)
            THEN 'wind'
            ELSE 'solar'
        END AS inferred_asset_type,
        
        -- Data quality
        ac.total_observations AS total_hourly_observations
        
    FROM asset_correlations ac
    CROSS JOIN daily_performance_scores ps
    LEFT JOIN rolling_correlations rc
        ON ac.asset_id = ps.asset_id
        AND ps.date = rc.date
    WHERE ac.asset_id = ps.asset_id
)

SELECT * FROM final_mart
