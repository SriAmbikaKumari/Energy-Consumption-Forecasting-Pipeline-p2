# Databricks notebook source
# MAGIC %md
# MAGIC 1: Read Bronze Table

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

energy_df = spark.table(
    "bronze.bronze_sch.bronze_energy_usage"
)

display(energy_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 2: Convert Data Types

# COMMAND ----------

energy_df = (
    energy_df
    .withColumn("voltage_reading", col("voltage_reading").cast(DoubleType()))
    .withColumn("current_reading", col("current_reading").cast(DoubleType()))
    .withColumn("active_power_kw", col("active_power_kw").cast(DoubleType()))
    .withColumn("reactive_power_kvar", col("reactive_power_kvar").cast(DoubleType()))
    .withColumn("energy_usage_kwh", col("energy_usage_kwh").cast(DoubleType()))
    .withColumn("frequency_hz", col("frequency_hz").cast(DoubleType()))
    .withColumn("load_factor", col("load_factor").cast(DoubleType()))
    .withColumn("peak_demand_kw", col("peak_demand_kw").cast(DoubleType()))
    .withColumn("offpeak_demand_kw", col("offpeak_demand_kw").cast(DoubleType()))
    .withColumn("daily_consumption_kwh", col("daily_consumption_kwh").cast(DoubleType()))
    .withColumn(
        "timestamp",
        to_timestamp(col("timestamp"), "dd-MM-yyyy HH:mm")
    )
)

display(energy_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 3: Remove Duplicate Records

# COMMAND ----------

energy_df = energy_df.dropDuplicates(
[
    "household_id",
    "timestamp"
])

display(energy_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 4: Handle Null Values

# COMMAND ----------

energy_df = energy_df.fillna({

    "voltage_reading":0.0,
    "current_reading":0.0,
    "active_power_kw":0.0,
    "reactive_power_kvar":0.0,
    "energy_usage_kwh":0.0,
    "frequency_hz":0.0,
    "load_factor":0.0,
    "peak_demand_kw":0.0,
    "offpeak_demand_kw":0.0,
    "daily_consumption_kwh":0.0

})

display(energy_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 5: Data Validation

# COMMAND ----------

energy_df = energy_df.filter(
    col("voltage_reading") >= 0
)

energy_df = energy_df.filter(
    col("current_reading") >= 0
)

energy_df = energy_df.filter(
    col("active_power_kw") >= 0
)

energy_df = energy_df.filter(
    col("reactive_power_kvar") >= 0
)

energy_df = energy_df.filter(
    col("energy_usage_kwh") >= 0
)

energy_df = energy_df.filter(
    col("frequency_hz").between(45,65)
)

energy_df = energy_df.filter(
    col("load_factor").between(0,1)
)

energy_df = energy_df.filter(
    col("peak_demand_kw") >= 0
)

energy_df = energy_df.filter(
    col("offpeak_demand_kw") >= 0
)

energy_df = energy_df.filter(
    col("daily_consumption_kwh") >= 0
)

display(energy_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 6: Standardize Data

# COMMAND ----------

energy_df = (
    energy_df
    .withColumn("region_name", upper(trim(col("region_name"))))
    .withColumn("city_name", initcap(trim(col("city_name"))))
    .withColumn("meter_type", upper(trim(col("meter_type"))))
    .withColumn("customer_category", upper(trim(col("customer_category"))))
    .withColumn("grid_zone", upper(trim(col("grid_zone"))))
)

display(energy_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 7: Generate Surrogate Key

# COMMAND ----------

energy_df = energy_df.withColumn(
    "energy_key",
    xxhash64(
        "household_id",
        "timestamp"
    )
)

display(energy_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 8: Create Audit Columns

# COMMAND ----------

energy_df = (
    energy_df
    .withColumn("silver_ingestion_time", current_timestamp())
    .withColumn("record_status", lit("ACTIVE"))
)

display(energy_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 9: Write Silver Table

# COMMAND ----------

energy_df.write \
.format("delta") \
.mode("overwrite") \
.option("mergeSchema","true") \
.saveAsTable(
"silver.silver_sch.Energy_Usage"
)

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 10: OPTIMIZE

# COMMAND ----------

spark.sql("""
OPTIMIZE silver.silver_sch.Energy_Usage
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 11: ZORDER

# COMMAND ----------

spark.sql("""
OPTIMIZE silver.silver_sch.Energy_Usage
ZORDER BY (household_id, timestamp)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 12: VACUUM

# COMMAND ----------

spark.sql("""
VACUUM silver.silver_sch.Energy_Usage
RETAIN 168 HOURS
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 13: Time Travel - History

# COMMAND ----------

display(
spark.sql("""
DESCRIBE HISTORY silver.silver_sch.Energy_Usage
""")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 14: Read Previous Version

# COMMAND ----------

display(
spark.sql("""
SELECT *
FROM silver.silver_sch.Energy_Usage
VERSION AS OF 0
""")
)

# COMMAND ----------

# MAGIC %md
# MAGIC