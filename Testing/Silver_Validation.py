# Databricks notebook source
tables = [
    "bronze.bronze_sch.energy_usage",
    "bronze.bronze_sch.weather_stream",
    "bronze.bronze_sch.device_metrics",
    "bronze.bronze_sch.grid_load",
    "bronze.bronze_sch.tariff_metrics"
]

print("=" * 60)
print("SILVER VALIDATION")
print("=" * 60)

failed = False

for table in tables:

    count = spark.table(table).count()

    if count > 0:
        print(f"✅ PASS : {table} ({count} rows)")
    else:
        print(f"❌ FAIL : {table}")
        failed = True

if failed:
    raise Exception("Silver Validation Failed")

print("\n🎉 Silver Validation Successful")