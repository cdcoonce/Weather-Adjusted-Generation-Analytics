{{
    config(
        materialized='table',
        schema='marts'
    )
}}

-- Weather-adjusted performance for weather-driven assets (wind, solar), plus
-- technology-appropriate scores for storage (battery) and thermal (gas):
--   * wind / solar : actual vs. weather-regression-expected generation
--   * battery      : realized round-trip efficiency (discharge / charge)
--   * gas          : heat-rate efficiency (best / realized heat rate)
-- ``inferred_asset_type`` is now the explicit ``asset_type`` carried from RAW,
-- not an inference from correlation strength.

WITH hourly_data AS (
    SELECT
        asset_id,
        asset_type,
        date,
        wind_speed_mps,
        ghi,
        net_generation_mwh,
        capacity_factor,
        asset_capacity_mw,
        charge_mwh,
        discharge_mwh,
        heat_rate_btu_kwh
    FROM {{ ref('int_asset_weather_join') }}
    WHERE is_complete_record = TRUE
),

-- Per-asset correlations + regression models (meaningful for wind/solar).
asset_correlations AS (
    SELECT
        asset_id,
        MAX(asset_type) AS asset_type,
        MAX(asset_capacity_mw) AS asset_capacity_mw,
        COUNT(*) AS total_observations,
        ROUND(AVG(net_generation_mwh), 4) AS avg_net_generation_mwh,
        ROUND(AVG(capacity_factor), 4) AS avg_capacity_factor,
        ROUND(AVG(wind_speed_mps), 2) AS avg_wind_speed_mps,
        ROUND(AVG(ghi), 2) AS avg_ghi,

        ROUND(CORR(wind_speed_mps, net_generation_mwh), 4) AS wind_generation_correlation,
        ROUND(CORR(ghi, net_generation_mwh), 4) AS solar_generation_correlation,
        ROUND(CORR(wind_speed_mps, capacity_factor), 4) AS wind_cf_correlation,
        ROUND(CORR(ghi, capacity_factor), 4) AS solar_cf_correlation,

        ROUND(REGR_SLOPE(net_generation_mwh, wind_speed_mps), 6) AS wind_regression_slope,
        ROUND(REGR_INTERCEPT(net_generation_mwh, wind_speed_mps), 6) AS wind_regression_intercept,
        ROUND(REGR_SLOPE(net_generation_mwh, ghi), 6) AS solar_regression_slope,
        ROUND(REGR_INTERCEPT(net_generation_mwh, ghi), 6) AS solar_regression_intercept,

        ROUND(POWER(CORR(wind_speed_mps, net_generation_mwh), 2), 4) AS wind_r_squared,
        ROUND(POWER(CORR(ghi, net_generation_mwh), 2), 4) AS solar_r_squared

    FROM hourly_data
    GROUP BY asset_id
),

-- Daily statistics for rolling trends.
daily_stats AS (
    SELECT
        asset_id,
        date,
        COUNT(*) AS hourly_observations,
        ROUND(AVG(net_generation_mwh), 4) AS daily_avg_net_generation_mwh,
        ROUND(SUM(net_generation_mwh), 4) AS daily_total_net_generation_mwh,
        ROUND(AVG(capacity_factor), 4) AS daily_avg_capacity_factor
    FROM hourly_data
    GROUP BY asset_id, date
),

rolling_correlations AS (
    SELECT
        asset_id,
        date,
        ROUND(
            AVG(daily_avg_net_generation_mwh) OVER (
                PARTITION BY asset_id ORDER BY date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ), 4
        ) AS rolling_7d_avg_generation,
        ROUND(
            AVG(daily_avg_capacity_factor) OVER (
                PARTITION BY asset_id ORDER BY date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ), 4
        ) AS rolling_7d_avg_cf,
        ROUND(
            AVG(daily_avg_net_generation_mwh) OVER (
                PARTITION BY asset_id ORDER BY date
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ), 4
        ) AS rolling_30d_avg_generation,
        ROUND(
            AVG(daily_avg_capacity_factor) OVER (
                PARTITION BY asset_id ORDER BY date
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ), 4
        ) AS rolling_30d_avg_cf
    FROM daily_stats
),

-- Weather-regression expected generation, per hour (wind/solar).
expected_generation AS (
    SELECT
        h.asset_id,
        h.date,
        h.net_generation_mwh AS actual_generation_mwh,
        ROUND(
            GREATEST(
                c.wind_regression_intercept + (c.wind_regression_slope * h.wind_speed_mps), 0
            ), 4
        ) AS expected_generation_from_wind,
        ROUND(
            GREATEST(
                c.solar_regression_intercept + (c.solar_regression_slope * h.ghi), 0
            ), 4
        ) AS expected_generation_from_solar,
        c.wind_generation_correlation,
        c.solar_generation_correlation
    FROM hourly_data h
    INNER JOIN asset_correlations c ON h.asset_id = c.asset_id
),

performance_scores AS (
    SELECT
        asset_id,
        date,
        CASE
            WHEN ABS(wind_generation_correlation) > ABS(solar_generation_correlation)
            THEN expected_generation_from_wind
            ELSE expected_generation_from_solar
        END AS expected_generation_mwh,
        actual_generation_mwh,
        ROUND(
            actual_generation_mwh / NULLIF(
                CASE
                    WHEN ABS(wind_generation_correlation) > ABS(solar_generation_correlation)
                    THEN expected_generation_from_wind
                    ELSE expected_generation_from_solar
                END, 0
            ) * 100, 2
        ) AS performance_ratio_pct
    FROM expected_generation
),

-- Wind/solar daily weather-adjusted aggregates.
daily_ws_scores AS (
    SELECT
        asset_id,
        date,
        ROUND(AVG(expected_generation_mwh), 4) AS avg_expected_generation_mwh,
        ROUND(AVG(actual_generation_mwh), 4) AS avg_actual_generation_mwh,
        ROUND(AVG(performance_ratio_pct), 2) AS avg_performance_ratio_pct
    FROM performance_scores
    GROUP BY asset_id, date
),

-- Technology-specific daily metrics for battery/gas scoring.
daily_tech AS (
    SELECT
        asset_id,
        date,
        SUM(charge_mwh) AS total_charge_mwh,
        SUM(discharge_mwh) AS total_discharge_mwh,
        MIN(heat_rate_btu_kwh) AS min_heat_rate,
        AVG(heat_rate_btu_kwh) AS avg_heat_rate
    FROM hourly_data
    GROUP BY asset_id, date
),

-- Combine into a single daily score, branched by technology.
daily_performance_scores AS (
    SELECT
        ac.asset_id,
        ws.date,
        ac.asset_type,
        ws.avg_expected_generation_mwh,
        ws.avg_actual_generation_mwh,
        ws.avg_performance_ratio_pct,
        ROUND(
            CASE ac.asset_type
                WHEN 'battery' THEN COALESCE(
                    LEAST(GREATEST(
                        dt.total_discharge_mwh / NULLIF(dt.total_charge_mwh, 0) * 100, 0
                    ), 100), 100
                )
                WHEN 'gas' THEN COALESCE(
                    LEAST(GREATEST(
                        dt.min_heat_rate / NULLIF(dt.avg_heat_rate, 0) * 100, 0
                    ), 100), 100
                )
                ELSE COALESCE(LEAST(GREATEST(ws.avg_performance_ratio_pct, 0), 100), 0)
            END, 2
        ) AS performance_score
    FROM asset_correlations ac
    INNER JOIN daily_ws_scores ws ON ac.asset_id = ws.asset_id
    LEFT JOIN daily_tech dt
        ON ws.asset_id = dt.asset_id AND ws.date = dt.date
),

final_mart AS (
    SELECT
        ac.asset_id,
        ps.date,

        -- Asset characteristics
        ac.asset_capacity_mw,

        -- Overall correlations (meaningful for wind/solar)
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
        CASE
            WHEN ps.performance_score >= 95 THEN 'Excellent'
            WHEN ps.performance_score >= 85 THEN 'Good'
            WHEN ps.performance_score >= 70 THEN 'Fair'
            ELSE 'Poor'
        END AS performance_category,

        -- Rolling metrics
        rc.rolling_7d_avg_generation,
        rc.rolling_7d_avg_cf,
        rc.rolling_30d_avg_generation,
        rc.rolling_30d_avg_cf,

        -- Explicit technology (carried from RAW, not inferred)
        ac.asset_type AS inferred_asset_type,

        -- Data quality
        ac.total_observations AS total_hourly_observations

    FROM asset_correlations ac
    INNER JOIN daily_performance_scores ps
        ON ac.asset_id = ps.asset_id
    LEFT JOIN rolling_correlations rc
        ON ac.asset_id = rc.asset_id AND ps.date = rc.date
)

SELECT * FROM final_mart
