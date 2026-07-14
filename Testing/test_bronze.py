import pytest

def test_bronze_energy_usage():

    df = spark.table("Bronze_Catalog.Bronze_SCH.Bronze_Energy_Usage")

    print("Energy Usage Records :", df.count())

    assert df.count() > 0


def test_bronze_weather():

    df = spark.table("Bronze_Catalog.Bronze_SCH.Bronze_Weather")

    print("Weather Records :", df.count())

    assert df.count() > 0


def test_bronze_device_metrics():

    df = spark.table("Bronze_Catalog.Bronze_SCH.Bronze_Device_Metrics")

    print("Device Metrics Records :", df.count())

    assert df.count() > 0


def test_bronze_grid_load():

    df = spark.table("Bronze_Catalog.Bronze_SCH.Bronze_Grid_Load")

    print("Grid Load Records :", df.count())

    assert df.count() > 0


def test_bronze_tariff_metrics():

    df = spark.table("Bronze_Catalog.Bronze_SCH.Bronze_Tariff_Metrics")

    print("Tariff Records :", df.count())

    assert df.count() > 0