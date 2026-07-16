# Databricks notebook source
audit_table = "bronze.bronze_sch.bronze_audit_logs"

count = spark.table(audit_table).count()

if count > 0:
    print(f"✅ PASS : Audit Log ({count} records)")
else:
    raise Exception("❌ Audit Log is Empty")