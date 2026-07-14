# Databricks notebook source
# MAGIC %md
# MAGIC Cell 1: Import Libraries

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 2: Read Bronze Table

# COMMAND ----------

device_df = spark.table(
    "bronze.bronze_sch.bronze_device_metrics"
)

display(device_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 3: Convert Data Types

# COMMAND ----------

device_df = (
    device_df
    .withColumn("runtime_hours", col("runtime_hours").cast(DoubleType()))
    .withColumn("device_power_kw", col("device_power_kw").cast(DoubleType()))
    .withColumn("motor_speed_rpm", col("motor_speed_rpm").cast(DoubleType()))
    .withColumn("efficiency_ratio", col("efficiency_ratio").cast(DoubleType()))
    .withColumn("energy_draw_kwh", col("energy_draw_kwh").cast(DoubleType()))
    .withColumn("heat_output", col("heat_output").cast(DoubleType()))
    .withColumn("cooling_load", col("cooling_load").cast(DoubleType()))
    .withColumn("device_voltage", col("device_voltage").cast(DoubleType()))
    .withColumn("device_current", col("device_current").cast(DoubleType()))
    .withColumn("device_temperature", col("device_temperature").cast(DoubleType()))
)

display(device_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 4: Remove Duplicate Records

# COMMAND ----------

device_df = device_df.dropDuplicates(
[
    "household_id",
    "device_model"
])

display(device_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 5: Handle Null Values

# COMMAND ----------

device_df = device_df.fillna({

    "runtime_hours":0.0,
    "device_power_kw":0.0,
    "motor_speed_rpm":0.0,
    "efficiency_ratio":0.0,
    "energy_draw_kwh":0.0,
    "heat_output":0.0,
    "cooling_load":0.0,
    "device_voltage":0.0,
    "device_current":0.0,
    "device_temperature":0.0

})

display(device_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 6: Data Validation

# COMMAND ----------

device_df = device_df.filter(
    col("runtime_hours") >= 0
)

device_df = device_df.filter(
    col("device_power_kw") >= 0
)

device_df = device_df.filter(
    col("motor_speed_rpm") >= 0
)

device_df = device_df.filter(
    col("energy_draw_kwh") >= 0
)

device_df = device_df.filter(
    col("heat_output") >= 0
)

device_df = device_df.filter(
    col("cooling_load") >= 0
)

device_df = device_df.filter(
    col("device_voltage") >= 0
)

device_df = device_df.filter(
    col("device_current") >= 0
)

device_df = device_df.filter(
    col("device_temperature") >= 0
)

device_df = device_df.filter(
    col("efficiency_ratio").between(0,1)
)

display(device_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 7: Standardize Data

# COMMAND ----------

device_df = (
    device_df
    .withColumn("device_category", upper(trim(col("device_category"))))
    .withColumn("device_brand", initcap(trim(col("device_brand"))))
    .withColumn("device_model", upper(trim(col("device_model"))))
    .withColumn("maintenance_status", upper(trim(col("maintenance_status"))))
    .withColumn("installation_region", initcap(trim(col("installation_region"))))
)

display(device_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Cell 8: Generate Surrogate Key

# COMMAND ----------

device_df = device_df.withColumn(
    "device_key",
    xxhash64(
        "household_id",
        "device_model"
    )
)

display(device_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 9: Create Audit Columns

# COMMAND ----------

device_df = (
    device_df
    .withColumn("silver_ingestion_time", current_timestamp())
    .withColumn("record_status", lit("ACTIVE"))
)

display(device_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 10: Write Silver Table

# COMMAND ----------

device_df.write \
.format("delta") \
.mode("overwrite") \
.option("mergeSchema","true") \
.saveAsTable(
    "silver.silver_sch.Device_Metrics"
)

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 11: OPTIMIZE

# COMMAND ----------

spark.sql("""
OPTIMIZE silver.silver_sch.Device_Metrics
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 12: ZORDER

# COMMAND ----------

spark.sql("""
OPTIMIZE silver.silver_sch.Device_Metrics
ZORDER BY (household_id, device_model)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 13: VACUUM

# COMMAND ----------

spark.sql("""
VACUUM silver.silver_sch.Device_Metrics
RETAIN 168 HOURS
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 14: Time Travel - History

# COMMAND ----------

display(
spark.sql("""
DESCRIBE HISTORY silver.silver_sch.Device_Metrics
""")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 15: Read Previous Version

# COMMAND ----------

display(
spark.sql("""
SELECT *
FROM silver.silver_sch.Device_Metrics
VERSION AS OF 0
""")
)