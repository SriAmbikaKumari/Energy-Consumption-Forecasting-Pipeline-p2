import pytest

# ----------------------------------
# Silver Energy Usage
# ----------------------------------

def test_silver_energy_usage():

    df = spark.table("Silver_Catalog.Silver_SCH.Silver_Energy_Usage")

    print("Silver Energy Usage Records :", df.count())

    assert df.count() > 0


# ----------------------------------
# Silver Weather
# ----------------------------------

def test_silver_weather():

    df = spark.table("Silver_Catalog.Silver_SCH.Silver_Weather")

    print("Silver Weather Records :", df.count())

    assert df.count() > 0


# ----------------------------------
# Silver Device Metrics
# ----------------------------------

def test_silver_device_metrics():

    df = spark.table("Silver_Catalog.Silver_SCH.Silver_Device_Metrics")

    print("Silver Device Metrics Records :", df.count())

    assert df.count() > 0


# ----------------------------------
# Silver Grid Load
# ----------------------------------

def test_silver_grid_load():

    df = spark.table("Silver_Catalog.Silver_SCH.Silver_Grid_Load")

    print("Silver Grid Load Records :", df.count())

    assert df.count() > 0


# ----------------------------------
# Silver Tariff Metrics
# ----------------------------------

def test_silver_tariff_metrics():

    df = spark.table("Silver_Catalog.Silver_SCH.Silver_Tariff_Metrics")

    print("Silver Tariff Records :", df.count())

    assert df.count() > 0


# ----------------------------------
# Audit Log
# ----------------------------------

def test_silver_audit_log():

    df = spark.table("Silver_Catalog.Silver_SCH.Audit_Log")

    print("Audit Log Records :", df.count())

    assert df.count() > 0


# ----------------------------------
# Time Travel
# ----------------------------------

def test_time_travel():

    history = spark.sql("""
    DESCRIBE HISTORY Silver_Catalog.Silver_SCH.Silver_Energy_Usage
    """)

    print("History Records :", history.count())

    assert history.count() > 0


# ----------------------------------
# Execute Tests (Notebook Demo)
# ----------------------------------

test_silver_energy_usage()
test_silver_weather()
test_silver_device_metrics()
test_silver_grid_load()
test_silver_tariff_metrics()
test_silver_audit_log()
test_time_travel()

print("All Silver PyTests Passed")