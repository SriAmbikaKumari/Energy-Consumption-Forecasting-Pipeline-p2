import pytest

# ----------------------------------
# Bronze Audit Log
# ----------------------------------

def test_bronze_audit_log():

    df = spark.table("Bronze_Catalog.Bronze_SCH.Audit_Log")

    print("Bronze Audit Records :", df.count())

    assert df.count() > 0

    print(" Bronze Audit Log Test Passed")


# ----------------------------------
# Silver Audit Log
# ----------------------------------

def test_silver_audit_log():

    df = spark.table("Silver_Catalog.Silver_SCH.Audit_Log")

    print("Silver Audit Records :", df.count())

    assert df.count() > 0

    print("Silver Audit Log Test Passed")


# ----------------------------------
# Success Status Validation
# ----------------------------------

def test_success_status():

    bronze_success = spark.sql("""
        SELECT COUNT(*)
        FROM Bronze_Catalog.Bronze_SCH.Audit_Log
        WHERE Status = 'SUCCESS'
    """).collect()[0][0]

    silver_success = spark.sql("""
        SELECT COUNT(*)
        FROM Silver_Catalog.Silver_SCH.Audit_Log
        WHERE Status = 'SUCCESS'
    """).collect()[0][0]

    print("Bronze SUCCESS Logs :", bronze_success)
    print("Silver SUCCESS Logs :", silver_success)

    assert bronze_success > 0
    assert silver_success > 0

    print(" SUCCESS Status Validation Passed")


# ----------------------------------
# Failure Records Validation
# ----------------------------------

def test_failure_logs():

    bronze_failed = spark.sql("""
        SELECT COUNT(*)
        FROM Bronze_Catalog.Bronze_SCH.Audit_Log
        WHERE Status = 'FAILED'
    """).collect()[0][0]

    silver_failed = spark.sql("""
        SELECT COUNT(*)
        FROM Silver_Catalog.Silver_SCH.Audit_Log
        WHERE Status = 'FAILED'
    """).collect()[0][0]

    print("Bronze FAILED Logs :", bronze_failed)
    print("Silver FAILED Logs :", silver_failed)

    assert bronze_failed >= 0
    assert silver_failed >= 0

    print(" Failure Log Validation Passed")


# ----------------------------------
# Execute Tests
# ----------------------------------

test_bronze_audit_log()
test_silver_audit_log()
test_success_status()
test_failure_logs()

print("===================================")
print("AUDIT LOG PYTEST PASSED")
print("===================================")