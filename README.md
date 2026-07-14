# ⚡ Energy Consumption Forecasting Pipeline

Production-ready Azure Data Engineering project for **Energy Consumption Analytics** using **Azure Data Lake Storage Gen2 (ADLS), Azure Data Factory, Azure Databricks (PySpark), Delta Lake, dbt Cloud, Databricks Workflows, Apache Airflow, and Power BI**.

The project implements an end-to-end ETL pipeline following the **Medallion Architecture (Bronze → Silver → Gold)** to transform raw energy consumption data into business-ready analytics.

---

# 📖 Project Overview

The **Energy Consumption Forecasting Pipeline** is an end-to-end Azure Data Engineering project designed to build a scalable and reliable Lakehouse architecture for energy analytics.

The pipeline automates data ingestion, transformation, validation, dimensional modeling, workflow orchestration, and dashboard reporting using Azure cloud services.

It follows the **Medallion Architecture** to progressively improve data quality and deliver trusted datasets for analytics.

---

# 🏗️ High Level Architecture


<img width="1536" height="1024" alt="Highlevel" src="https://github.com/user-attachments/assets/a444fe27-8e0f-46d8-bc37-9546e2ae61f4" />

---

# 🏛️ Medallion Architecture

<img width="1536" height="1024" alt="Medallion_architectue" src="https://github.com/user-attachments/assets/88fd0cf8-cacf-4e67-9acf-1c19e75860c9" />

---

# ⚙️ Technology Stack

| Category | Technologies |
|-----------|--------------|
| Cloud | Azure |
| Storage | Azure Data Lake Storage Gen2 |
| ETL | Azure Data Factory |
| Processing | Azure Databricks |
| Language | PySpark |
| Data Lake | Delta Lake |
| Data Modeling | dbt Cloud |
| Warehouse | Databricks SQL Warehouse |
| Orchestration | Databricks Workflows, Apache Airflow |
| Reporting | Power BI |
| Version Control | Git, GitHub |
| Testing | Pytest |

---

# 📂 Project Workflow

```text
Kaggle Dataset
        │
        ▼
Azure Data Lake Storage (CSV)
        │
        ▼
Azure Data Factory
(CSV → Parquet)
        │
        ▼
Azure Data Lake Storage
(Parquet Files)
        │
        ▼
Azure Databricks
        │
        ▼
Bronze Layer
        │
        ▼
Silver Layer
        │
        ▼
dbt Cloud
(Staging → Dimensions → Fact → Marts)
        │
        ▼
Gold Layer
        │
        ▼
Databricks SQL Warehouse
        │
        ▼
Power BI Dashboard
```

---

# 🚀 Project Implementation

## Step 1 – Data Ingestion

- Created Azure Storage Account
- Created ADLS Gen2 Container
- Uploaded Energy Consumption CSV files
- Verified successful ingestion

---

## Step 2 – Azure Data Factory

- Created Linked Services
- Configured Source Dataset
- Configured Sink Dataset
- Converted CSV files into Parquet
- Stored transformed files in ADLS

---

## 🥉 Bronze Layer

### Objective

Store raw data in Delta format.

### Tables

- Bronze Energy Usage
- Bronze Weather Stream
- Bronze Device Metrics
- Bronze Grid Load
- Bronze Tariff Metrics

### Features

- Metadata Columns
- Audit Columns
- Schema Evolution
- Watermarking
- Error Logging

---

## 🥈 Silver Layer

### Objective

Clean and validate Bronze data.

### Tables

- Energy Usage
- Weather Stream
- Device Metrics
- Grid Load
- Tariff Metrics

### Transformations

- Remove Duplicates
- Handle Null Values
- Standardize Data
- Data Type Conversion
- Surrogate Keys
- SCD Type 2
- Data Validation
- Delta Optimization

---

## 🥇 Gold Layer (dbt Cloud)

### Staging Models

- stg_energy
- stg_weather
- stg_device
- stg_grid
- stg_tariff

### Dimension Models

- dim_date
- dim_household
- dim_weather
- dim_device
- dim_grid
- dim_tariff

### Fact Model

- fact_energy

### Mart Models

- daily_summary
- monthly_summary
- yearly_summary

### Dashboard Model

- executive_dashboard

---

# 🔄 Pipeline Orchestration

The complete pipeline is orchestrated using:

- Azure Data Factory
- Databricks Workflows
- Apache Airflow
- dbt Cloud

---

# 🧪 Testing

- Pytest
- dbt Tests
- SQL Validation
- Data Quality Checks

---

# 📊 Business Dashboards

## ⚡ Energy Consumption Dashboard

<img width="6000" height="2941" alt="executive_dashboard" src="https://github.com/user-attachments/assets/2d5da4fd-d5a2-4bdc-9ae6-6982ee9f6fce" />

Tracks total and average energy usage across regions, cities, and customer categories.

---

## 🌦 Weather Impact Dashboard
![Uploading weather_impact_dashboard.jpg…]()


Analyzes the relationship between weather conditions and energy consumption.

---

## 🔌 Device Monitoring Dashboard

<img width="6000" height="5125" alt="billing_analytics_dashboard" src="https://github.com/user-attachments/assets/eb53f3ec-9e5e-4f90-81a2-420317519a49" />

Monitors runtime, efficiency, and operational health of connected devices.

---

## 💰 Billing Analytics Dashboard
<img width="6000" height="5125" alt="billing_analytics_dashboard" src="https://github.com/user-attachments/assets/7170bb6e-5986-4d9f-8af3-424afece76f8" />


Analyzes billing, tariff plans, and revenue across regions and customer categories.

---

# 📁 Project Structure

```text
Energy-Consumption-Forecasting-Pipeline

│── Architecture
│── Datasets
│── ADF
│── Bronze
│── Silver
│── Gold
│── Airflow
│── Dashboard
│── Testing
│── docs
└── README.md
```

---

# 📸 Project Screenshots

## Azure Data Factory

<p align="center">
<img src="docs/adf_pipeline.png" width="900">
</p>

---

## Databricks Workflow

<p align="center">
<img src="docs/databricks_workflow.png" width="900">
</p>

---

## dbt Lineage

<p align="center">
<img src="docs/dbt_lineage.png" width="900">
</p>

---

## Power BI Dashboard

<p align="center">
<img src="docs/powerbi_dashboard.png" width="900">
</p>

---

# 🎯 Business Outcomes

- Automated end-to-end ETL pipeline
- Improved data quality through Medallion Architecture
- Automated workflow orchestration
- Business-ready Gold Layer
- Interactive dashboards
- Scalable Lakehouse implementation
- Analytics-ready datasets for future forecasting

---

# 👨‍💻 Author

**Veligarapu Sri Ambika Kumari**

Azure Data Engineering Project
