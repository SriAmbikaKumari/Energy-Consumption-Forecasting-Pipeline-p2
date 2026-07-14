import pytest

# ----------------------------------
# Dimension Tables
# ----------------------------------

def test_dim_household():

    df = spark.table("gold_catalog.gold_sch_dbt.dim_household")

    print("Dim Household Records :", df.count())

    assert df.count() > 0


def test_dim_date():

    df = spark.table("gold_catalog.gold_sch_dbt.dim_date")

    print("Dim Date Records :", df.count())

    assert df.count() > 0


def test_dim_region():

    df = spark.table("gold_catalog.gold_sch_dbt.dim_region")

    print("Dim Region Records :", df.count())

    assert df.count() > 0


def test_dim_device():

    df = spark.table("gold_catalog.gold_sch_dbt.dim_device")

    print("Dim Device Records :", df.count())

    assert df.count() > 0


def test_dim_tariff():

    df = spark.table("gold_catalog.gold_sch_dbt.dim_tariff")

    print("Dim Tariff Records :", df.count())

    assert df.count() > 0


# ----------------------------------
# Fact Tables
# ----------------------------------

def test_fact_energy_usage():

    df = spark.table("gold_catalog.gold_sch_dbt.fact_energy_usage")

    print("Fact Energy Usage Records :", df.count())

    assert df.count() > 0


def test_fact_grid_load():

    df = spark.table("gold_catalog.gold_sch_dbt.fact_grid_load")

    print("Fact Grid Load Records :", df.count())

    assert df.count() > 0


# ----------------------------------
# Summary Tables
# ----------------------------------

def test_daily_summary():

    df = spark.table("gold_catalog.gold_sch_dbt.daily_energy_summary")

    print("Daily Summary Records :", df.count())

    assert df.count() > 0


def test_monthly_summary():

    df = spark.table("gold_catalog.gold_sch_dbt.monthly_energy_summary")

    print("Monthly Summary Records :", df.count())

    assert df.count() > 0


def test_yearly_summary():

    df = spark.table("gold_catalog.gold_sch_dbt.yearly_energy_summary")

    print("Yearly Summary Records :", df.count())

    assert df.count() > 0


def test_region_summary():

    df = spark.table("gold_catalog.gold_sch_dbt.region_energy_summary")

    print("Region Summary Records :", df.count())

    assert df.count() > 0


def test_device_summary():

    df = spark.table("gold_catalog.gold_sch_dbt.device_energy_summary")

    print("Device Summary Records :", df.count())

    assert df.count() > 0


# ----------------------------------
# KPI Tables
# ----------------------------------

def test_energy_summary():

    df = spark.table("gold_catalog.gold_sch_dbt.energy_summary")

    print("Energy Summary Records :", df.count())

    assert df.count() > 0


def test_customer_kpi():

    df = spark.table("gold_catalog.gold_sch_dbt.customer_kpi")

    print("Customer KPI Records :", df.count())

    assert df.count() > 0


def test_grid_kpi():

    df = spark.table("gold_catalog.gold_sch_dbt.grid_kpi")

    print("Grid KPI Records :", df.count())

    assert df.count() > 0


def test_weather_kpi():

    df = spark.table("gold_catalog.gold_sch_dbt.weather_kpi")

    print("Weather KPI Records :", df.count())

    assert df.count() > 0


def test_executive_dashboard():

    df = spark.table("gold_catalog.gold_sch_dbt.executive_dashboard")

    print("Executive Dashboard Records :", df.count())

    assert df.count() > 0


# ----------------------------------
# Execute Tests (Notebook Demo)
# ----------------------------------

test_dim_household()
test_dim_date()
test_dim_region()
test_dim_device()
test_dim_tariff()

test_fact_energy_usage()
test_fact_grid_load()

test_daily_summary()
test_monthly_summary()
test_yearly_summary()
test_region_summary()
test_device_summary()

test_energy_summary()
test_customer_kpi()
test_grid_kpi()
test_weather_kpi()
test_executive_dashboard()

print("===================================")
print("ALL GOLD PYTESTS PASSED")
print("===================================")