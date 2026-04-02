CREATE OR REPLACE TABLE fact_engine_events AS

WITH hourly_metrics AS (
    SELECT
        engine_id,
        loc_id,
        date_trunc('hour', event_timestamp) AS event_hour,
        AVG(temp) AS avg_temp,
        MAX(temp) AS max_temp,
        AVG(vibration) AS avg_vibration,
        SUM(CASE WHEN status_code != 'OK' THEN 1 ELSE 0 END) AS warning_count
    FROM silver_engine_telemetry
    GROUP BY engine_id, loc_id, event_hour
)

SELECT
    de.engine_key,
    dl.location_key,
    dt.event_hour,
    hm.avg_temp,
    hm.max_temp,
    hm.avg_vibration,
    hm.warning_count
FROM hourly_metrics hm
LEFT JOIN dim_engine de ON hm.engine_id = de.engine_id
LEFT JOIN dim_location dl ON hm.loc_id = dl.loc_id
LEFT JOIN dim_time dt ON hm.event_hour = dt.event_hour;