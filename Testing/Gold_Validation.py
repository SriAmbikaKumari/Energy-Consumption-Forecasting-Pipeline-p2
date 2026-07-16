# Databricks notebook source
tables = [
    "gold.demo_schema.fact_energy",
    "gold.demo_schema.dim_date",
    "gold.demo_schema.dim_device",
    "gold.demo_schema.dim_grid",
    "gold.demo_schema.dim_household",
    "gold.demo_schema.dim_tariff",
    "gold.demo_schema.dim_weather",
    "gold.demo_schema.daily_summary",
    "gold.demo_schema.monthly_summary",
    "gold.demo_schema.yearly_summary",
    "gold.demo_schema.executive_dashboard"
]

print("=" * 60)
print("GOLD VALIDATION")
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
    raise Exception("Gold Validation Failed")

print("\n🎉 Gold Validation Successful")