from pyspark.sql import functions as F
from pyspark.sql.types import *
import random

num_rows = 100000
df_base = spark.range(num_rows)

raw_telemetry = df_base.withColumn("engine_id", 
    F.when(F.col("id") % 1000 == 0, F.lit(None))
    .when(F.col("id") % 2 == 0, F.lit("J920-001"))
    .when(F.col("id") % 3 == 0, F.lit("J920-002"))
    .otherwise(F.lit("J624-001"))
).withColumn("loc_id", 
    F.when(F.col("id") % 2 == 0, F.lit("LOC-01")).otherwise(F.lit("LOC-02"))
).withColumn("event_timestamp", 
    F.expr("current_timestamp() - cast(id as interval second)")
).withColumn("sensor_payload", 
    F.to_json(F.struct(
        F.when(F.col("id") % 5000 == 0, F.lit(9999.0))
         .when(F.col("id") % 7000 == 0, F.lit(-100.0))
         .otherwise(F.round(F.rand() * 50 + 400, 2)).alias("temp"),
        F.round(F.rand() * 5 + 10, 2).alias("vibration"),
        F.when(F.col("id") % 2000 == 0, F.lit("WARN-1X")).otherwise(F.lit("OK")).alias("status_code")
    ))
)

duplicates = raw_telemetry.limit(250)
final_telemetry = raw_telemetry.union(duplicates)

final_telemetry.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("raw_telemetry")

engine_data = [
    ("J920-001", "jenbacher j920", 2022),
    ("J920-002", "Jenbacher J920", None),
    ("J624-001", "JENBACHER j624", 2020),
    ("J624-002", "Jenbacher J624", 2023),
    (None, "Ismeretlen", 2010) 
]
spark.createDataFrame(engine_data, ["engine_id", "model", "build_year"]).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("raw_engines")

location_data = [
    ("LOC-01", "Hungary", "Veresegyhaz"),
    ("LOC-02", "Austria", "Jenbach"),
    ("LOC-03", "USA", "Waukesha")
]
spark.createDataFrame(location_data, ["loc_id", "country", "city"]).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("raw_locations")

maint_data = [
    ("M1", "Olajcsere", 500),
    ("M2", "Gyújtógyertya csere", 2000),
    ("M3", "Teljes generál", 10000)
]
spark.createDataFrame(maint_data, ["maint_type_id", "description", "interval_hours"]).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("raw_maint_types")