# Databricks notebook source
# MAGIC %md
# MAGIC 1: Read Bronze Table

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

grid_df = spark.table(
    "bronze.bronze_sch.bronze_grid_Load"
)

display(grid_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 2: Convert Data Types

# COMMAND ----------

grid_df = (
    grid_df
    .withColumn("grid_voltage", col("grid_voltage").cast(DoubleType()))
    .withColumn("grid_current", col("grid_current").cast(DoubleType()))
    .withColumn("grid_load_kw", col("grid_load_kw").cast(DoubleType()))
    .withColumn("transformer_load", col("transformer_load").cast(DoubleType()))
    .withColumn("line_loss_percent", col("line_loss_percent").cast(DoubleType()))
    .withColumn("load_variation", col("load_variation").cast(DoubleType()))
    .withColumn("frequency_variation", col("frequency_variation").cast(DoubleType()))
    .withColumn("grid_capacity_kw", col("grid_capacity_kw").cast(DoubleType()))
    .withColumn("demand_forecast_kw", col("demand_forecast_kw").cast(DoubleType()))
    .withColumn("reserve_margin", col("reserve_margin").cast(DoubleType()))
)

display(grid_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 3: Remove Duplicate Records

# COMMAND ----------

grid_df = grid_df.dropDuplicates(
[
    "household_id",
    "substation_name",
    "feeder_line"
])

display(grid_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 4: Handle Null Values

# COMMAND ----------

grid_df = grid_df.fillna({

    "grid_voltage":0.0,
    "grid_current":0.0,
    "grid_load_kw":0.0,
    "transformer_load":0.0,
    "line_loss_percent":0.0,
    "load_variation":0.0,
    "frequency_variation":0.0,
    "grid_capacity_kw":0.0,
    "demand_forecast_kw":0.0,
    "reserve_margin":0.0

})

display(grid_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 5: Data Validation

# COMMAND ----------

grid_df = grid_df.filter(
    col("grid_voltage") >= 0
)

grid_df = grid_df.filter(
    col("grid_current") >= 0
)

grid_df = grid_df.filter(
    col("grid_load_kw") >= 0
)

grid_df = grid_df.filter(
    col("transformer_load") >= 0
)

grid_df = grid_df.filter(
    col("line_loss_percent").between(0,100)
)

grid_df = grid_df.filter(
    col("grid_capacity_kw") >= 0
)

grid_df = grid_df.filter(
    col("demand_forecast_kw") >= 0
)

display(grid_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 6: Standardize Data
# MAGIC

# COMMAND ----------

grid_df = (
    grid_df
    .withColumn("grid_region", upper(trim(col("grid_region"))))
    .withColumn("substation_name", upper(trim(col("substation_name"))))
    .withColumn("feeder_line", upper(trim(col("feeder_line"))))
    .withColumn("distribution_zone", initcap(trim(col("distribution_zone"))))
    .withColumn("grid_operator", initcap(trim(col("grid_operator"))))
)

display(grid_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 7: Generate Surrogate Key

# COMMAND ----------

grid_df = grid_df.withColumn(
    "grid_key",
    xxhash64(
        "household_id",
        "substation_name",
        "feeder_line"
    )
)

display(grid_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 8: Create Audit Columns
# MAGIC

# COMMAND ----------

grid_df = (
    grid_df
    .withColumn("silver_ingestion_time", current_timestamp())
    .withColumn("record_status", lit("ACTIVE"))
)

display(grid_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 9: Write Silver Table

# COMMAND ----------

grid_df.write \
.format("delta") \
.mode("overwrite") \
.option("mergeSchema","true") \
.saveAsTable(
"silver.silver_sch.Grid_Load"
)

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 10: OPTIMIZE

# COMMAND ----------

spark.sql("""
OPTIMIZE silver.silver_sch.Grid_Load
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 11: ZORDER

# COMMAND ----------

spark.sql("""
OPTIMIZE silver.silver_sch.Grid_Load
ZORDER BY (household_id, substation_name)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 12: VACUUM

# COMMAND ----------

spark.sql("""
VACUUM silver.silver_sch.Grid_Load
RETAIN 168 HOURS
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 13: Time Travel History

# COMMAND ----------



# COMMAND ----------

display(
spark.sql("""
DESCRIBE HISTORY silver.silver_sch.Grid_Load
""")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 14: Read Previous Version

# COMMAND ----------

display(
spark.sql("""
SELECT *
FROM silver.silver_sch.Grid_Load
VERSION AS OF 0
""")
)