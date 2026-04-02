SELECT COUNT(*) from silver_engine_telemetry where engine_id is NULL
SELECT engine_id, event_timestamp, COUNT(*) from silver_engine_telemetry group by engine_id,event_timestamp HAVING COUNT(*) > 1
SELECT COUNT(*) from silver_engine_telemetry where temp not BETWEEN 200 and 800
