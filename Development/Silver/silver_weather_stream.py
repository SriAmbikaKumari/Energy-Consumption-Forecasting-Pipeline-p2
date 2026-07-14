# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

weather_df = spark.table("bronze.bronze_sch.bronze_weather_stream")

# COMMAND ----------

weather_df.printSchema()

# COMMAND ----------

weather_df.select("timestamp").show(10, False)

# COMMAND ----------

weather_df = weather_df.dropDuplicates(
    [
        "household_id",
        "timestamp"
    ]
)

display(weather_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 5: Handle Null Values

# COMMAND ----------

weather_df = weather_df.fillna({
    "temperature_celsius": 0.0,
    "humidity_percent": 0.0,
    "wind_speed_kmh": 0.0,
    "rainfall_mm": 0.0,
    "pressure_hpa": 0.0,
    "solar_radiation": 0.0,
    "dew_point": 0.0,
    "uv_index": 0.0,
    "visibility_km": 0.0,
    "cloud_cover_percent": 0.0
})
display(weather_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 7: Standardize Data

# COMMAND ----------

\
weather_df = (
    weather_df
    .withColumn("weather_region", initcap(trim(col("weather_region"))))
    .withColumn("weather_city", initcap(trim(col("weather_city"))))
    .withColumn("weather_station", upper(trim(col("weather_station"))))
    .withColumn("climate_zone", upper(trim(col("climate_zone"))))
    .withColumn("condition_type", upper(trim(col("condition_type"))))
)

display(weather_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 8: Generate Surrogate Key

# COMMAND ----------

weather_df = weather_df.withColumn(
    "weather_key",
    monotonically_increasing_id()
)

display(weather_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 9: Create Audit Columns

# COMMAND ----------

weather_df = (
    weather_df
    .withColumn("silver_ingestion_time", current_timestamp())
    .withColumn("record_status", lit("ACTIVE"))
)

display(weather_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 10: Write Silver Table

# COMMAND ----------

weather_df.write \
.format("delta") \
.mode("overwrite") \
.option("mergeSchema", "true") \
.saveAsTable(
    "silver.silver_sch.Weather_Stream"
)

# COMMAND ----------

spark.sql("""
OPTIMIZE silver.silver_sch.Weather_Stream
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 11: zorder

# COMMAND ----------

spark.sql("""
OPTIMIZE silver.silver_sch.Weather_Stream
ZORDER BY (household_id, timestamp)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 13: VACUUM

# COMMAND ----------

spark.sql("""
VACUUM silver.silver_sch.Weather_Stream
RETAIN 168 HOURS
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 14: View History

# COMMAND ----------

display(
    spark.sql("""
    DESCRIBE HISTORY silver.silver_sch.Weather_Stream
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 15: Time Travel

# COMMAND ----------

display(
    spark.sql("""
    SELECT *
    FROM silver.silver_sch.Weather_Stream
    VERSION AS OF 0
    """)
)