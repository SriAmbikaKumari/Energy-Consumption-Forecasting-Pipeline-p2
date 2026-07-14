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

tariff_df = spark.table(
    "bronze.bronze_sch.bronze_tariff_metrics"
)

display(tariff_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 3: Convert Data Types

# COMMAND ----------

tariff_df = (
    tariff_df
    .withColumn("unit_rate", col("unit_rate").cast(DoubleType()))
    .withColumn("peak_rate", col("peak_rate").cast(DoubleType()))
    .withColumn("offpeak_rate", col("offpeak_rate").cast(DoubleType()))
    .withColumn("fixed_charge", col("fixed_charge").cast(DoubleType()))
    .withColumn("tax_amount", col("tax_amount").cast(DoubleType()))
    .withColumn("subsidy_amount", col("subsidy_amount").cast(DoubleType()))
    .withColumn("monthly_bill", col("monthly_bill").cast(DoubleType()))
    .withColumn("billing_units", col("billing_units").cast(DoubleType()))
    .withColumn("late_fee", col("late_fee").cast(DoubleType()))
    .withColumn("adjustment_amount", col("adjustment_amount").cast(DoubleType()))
)

display(tariff_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 4: Remove Duplicate Records

# COMMAND ----------

tariff_df = tariff_df.dropDuplicates(
[
    "household_id",
    "billing_cycle"
])

display(tariff_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 5: Handle Null Values

# COMMAND ----------

tariff_df = tariff_df.fillna({

    "unit_rate":0.0,
    "peak_rate":0.0,
    "offpeak_rate":0.0,
    "fixed_charge":0.0,
    "tax_amount":0.0,
    "subsidy_amount":0.0,
    "monthly_bill":0.0,
    "billing_units":0.0,
    "late_fee":0.0,
    "adjustment_amount":0.0

})

display(tariff_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 6: Data Validation

# COMMAND ----------

tariff_df = tariff_df.filter(
    col("unit_rate") >= 0
)

tariff_df = tariff_df.filter(
    col("peak_rate") >= 0
)

tariff_df = tariff_df.filter(
    col("offpeak_rate") >= 0
)

tariff_df = tariff_df.filter(
    col("fixed_charge") >= 0
)

tariff_df = tariff_df.filter(
    col("tax_amount") >= 0
)

tariff_df = tariff_df.filter(
    col("subsidy_amount") >= 0
)

tariff_df = tariff_df.filter(
    col("monthly_bill") >= 0
)

tariff_df = tariff_df.filter(
    col("billing_units") >= 0
)

tariff_df = tariff_df.filter(
    col("late_fee") >= 0
)

tariff_df = tariff_df.filter(
    col("adjustment_amount") >= 0
)

display(tariff_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 7: Standardize Data

# COMMAND ----------

tariff_df = (
    tariff_df
    .withColumn("tariff_region", initcap(trim(col("tariff_region"))))
    .withColumn("tariff_city", initcap(trim(col("tariff_city"))))
    .withColumn("tariff_plan_type", upper(trim(col("tariff_plan_type"))))
    .withColumn("billing_cycle", upper(trim(col("billing_cycle"))))
    .withColumn("utility_provider", initcap(trim(col("utility_provider"))))
)

display(tariff_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 8: Generate Surrogate Key

# COMMAND ----------

tariff_df = tariff_df.withColumn(
    "tariff_key",
    xxhash64(
        "household_id",
        "billing_cycle"
    )
)

display(tariff_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 9: Create Audit Columns

# COMMAND ----------

tariff_df = (
    tariff_df
    .withColumn("silver_ingestion_time", current_timestamp())
    .withColumn("record_status", lit("ACTIVE"))
)

display(tariff_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 10: Write Silver Table

# COMMAND ----------

tariff_df.write \
.format("delta") \
.mode("overwrite") \
.option("mergeSchema","true") \
.saveAsTable(
    "silver.silver_sch.Tariff_Metrics"
)

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 11: OPTIMIZE

# COMMAND ----------

spark.sql("""
OPTIMIZE silver.silver_sch.Tariff_Metrics
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 12: ZORDER

# COMMAND ----------

spark.sql("""
OPTIMIZE silver.silver_sch.Tariff_Metrics
ZORDER BY (household_id, billing_cycle)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 13: VACUUM

# COMMAND ----------

spark.sql("""
VACUUM silver.silver_sch.Tariff_Metrics
RETAIN 168 HOURS
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 14: Time Travel - History

# COMMAND ----------

display(
spark.sql("""
DESCRIBE HISTORY silver.silver_sch.Tariff_Metrics
""")
)


# COMMAND ----------

# MAGIC %md
# MAGIC Cell 15: Read Previous Version

# COMMAND ----------


display(
spark.sql("""
SELECT *
FROM silver.silver_sch.Tariff_Metrics
VERSION AS OF 0
""")
)