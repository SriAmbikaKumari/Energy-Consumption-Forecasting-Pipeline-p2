# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze.bronze_sch;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS silver.silver_sch;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS gold.gold_sch;

# COMMAND ----------

# Configure connection to your Azure Storage account
spark.conf.set(
    "fs.azure.account.key.energyystorage.dfs.core.windows.net",
    "LWV4YcS2Hz53IcGZj27TCE9KA2bw91fUX1QLs2yrIIz1eqR1eCTCEhcbjdpn0CxkqPEBXTY6kmJj+AStI88gGg=="
)

print("Storage account authentication configured successfully.")

# COMMAND ----------

base_transformed_path = "abfss://energycontainer@energyystorage.dfs.core.windows.net/transformed/"

energy_df = (
    spark.read
    .format("parquet")
    .load(base_transformed_path + "energy_usage_stream_v2.parquet")
)

display(energy_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 4: Add Bronze Metadata

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, expr

energy_bronze = (
    energy_df
    .withColumn("bronze_ingestion_time", current_timestamp())
    .withColumn("source_file", expr("_metadata.file_path"))
    .withColumn("batch_id", expr("uuid()"))
)

display(energy_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 5: Watermark

# COMMAND ----------

energy_bronze = (
    energy_bronze
    .withWatermark("reading_timestamp", "2 hours")
)
display(energy_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 6: Enable Schema Evolution

# COMMAND ----------

spark.conf.set(
    "spark.databricks.delta.schema.autoMerge.enabled",
    "true"
)

print("Schema evolution enabled successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 7: Load Bronze Table

# COMMAND ----------


target_table = "bronze.bronze_sch.bronze_energy_usage"

try:

    (
        energy_bronze.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(target_table)
    )

    print(f"Successfully loaded into {target_table}")

except Exception as e:

    print(f"Bronze Load Failed : {target_table}")
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 8: View Delta History

# COMMAND ----------

spark.sql(f"DESCRIBE HISTORY {target_table}").display()

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 9: Create Audit Table

# COMMAND ----------

# Create the central log table if it doesn't exist yet
spark.sql("""
    CREATE TABLE IF NOT EXISTS bronze.bronze_sch.bronze_audit_logs (
        process_name STRING,
        source_file STRING,
        target_table STRING,
        records_processed LONG,
        status STRING,
        error_message STRING,
        log_timestamp TIMESTAMP
    )
    USING delta
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Cell 10: Create Error Log Table

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, expr
from datetime import datetime
import sys

PROCESS_NAME = "Ingest_Energy_Usage"
FILE_NAME = "energy_usage_stream_v2.parquet"
TARGET_TABLE = "bronze.bronze_sch.bronze_energy_usage"

try:
    # 1. Read Data (Assuming energy_df is your spark.read dataframe)
    energy_bronze = (
        energy_df
        .withColumn("bronze_ingestion_time", current_timestamp())
        .withColumn("source_file", expr("_metadata.file_path"))
    )
    
    # 2. Write Data to Delta
    energy_bronze.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(TARGET_TABLE)
        
    # 3. Capture Row Count for Audit
    inserted_rows = energy_bronze.count()
    
    # 4. Write SUCCESS Log (Using standard datetime.now() instead of the PySpark function)
    current_time = datetime.now()
    audit_df = spark.createDataFrame([(
        PROCESS_NAME, FILE_NAME, TARGET_TABLE, inserted_rows, "SUCCESS", None, current_time
    )], schema="process_name STRING, source_file STRING, target_table STRING, records_processed LONG, status STRING, error_message STRING, log_timestamp TIMESTAMP")
    
    audit_df.write.format("delta").mode("append").saveAsTable("bronze.bronze_sch.bronze_audit_logs")
    print(f"Successfully processed {inserted_rows} records.")

except Exception as e:
    # 5. Write FAILURE Log if anything breaks
    error_msg = str(e)
    current_time = datetime.now()
    
    error_df = spark.createDataFrame([(
        PROCESS_NAME, FILE_NAME, TARGET_TABLE, 0, "FAILED", error_msg, current_time
    )], schema="process_name STRING, source_file STRING, target_table STRING, records_processed LONG, status STRING, error_message STRING, log_timestamp TIMESTAMP")
    
    error_df.write.format("delta").mode("append").saveAsTable("bronze2.bronze_sch.bronze_audit_logs")
    print(f"Pipeline Failed: {error_msg}")