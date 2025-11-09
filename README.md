# 🏥 **Greenville Hospital Data Engineering & Analytics Project**

---

## 🧭 **Project Overview**

The **Greenville Hospital ETL & Analytics Project** is an **end-to-end data engineering pipeline** designed to extract, clean, transform, and load patient encounter data from hospital systems into an analytics-ready format.

This project automates:
- ✅ Data validation  
- ✅ Outlier detection  
- ✅ Timestamp consistency checks  
- ✅ Reporting integration via Power BI / Tableau  

It leverages **Azure Data Factory**, **Azure Databricks (PySpark)**, and **Azure Data Lake Storage (ADLS Gen2)** to ensure data quality and transparency across the hospital’s operations.

---

## ⚙️ **Core Technologies Used**

| Platform | Purpose |
|-----------|----------|
| 🔹 **Azure Data Factory (ADF)** | Pipeline orchestration for ingestion and cleaning |
| 🔹 **Azure Databricks (PySpark)** | Transformation, validation, and analytics |
| 🔹 **Azure Data Lake Storage (ADLS Gen2)** | Centralized data lake for raw and curated layers |
| 🔹 **SQL Server / Azure SQL Database** | Analytical storage for BI |
| 🔹 **Power BI / Tableau** | Visualization and dashboarding |
| 🔹 **Loguru** | Logging and ETL monitoring |

---

## 🎯 **Business Purpose**

Hospitals generate large amounts of encounter and claim data often containing inconsistencies such as:
- Missing **PatientIDs**
- Invalid **Start / Stop timestamps**
- Outliers in **Total Claim Cost** and **Payer Coverage**

This project ensures high-quality, consistent data for:
- 💰 **Finance Teams** — monitor claim cost trends  
- 🏥 **Operations** — understand encounter flow and resource efficiency  
- 📊 **Analytics Teams** — generate reliable KPIs for executive dashboards  

---

## 📈 **Business Outcomes**

- ⏱️ Reduced manual data-cleaning workload  
- 📈 Improved accuracy in financial and operational reports  
- 🧩 Established a foundation for predictive analytics  

---

## 🗺️ **Data Architecture Flow**

### **Flow Explanation**

**Raw Data (Azure Blob Storage):**  
Uncleaned patient encounter data from multiple sources (CSV / API / EHR) is ingested into the raw zone.  

**Transformation (Azure Data Factory):**  
Data is cleaned, standardized, and normalized through ADF pipelines.  

**Load Clean Data (Azure Blob Storage):**  
Validated and structured data stored in the curated layer for analysis.  

**Analysis (Azure Databricks):**  
Databricks performs advanced analytics — detecting outliers, validating timestamps, and computing insights.  

---

## ⚙️ **ETL Workflow Summary**

| Stage | Component | Description |
|--------|------------|-------------|
| **Extract** | Azure Data Factory Pipelines | Pulls raw encounter data into ADLS Gen2 |
| **Transform** | PySpark (Databricks) | Cleans nulls, validates timestamps, detects outliers |
| **Load** | Azure Data Lake / SQL | Loads curated data into storage for reporting |
| **Validate & Log** | Python + Loguru | Tracks ETL executions and anomalies |
| **Visualize** | Power BI / Tableau | Presents claim-cost and patient-flow metrics |

---

## 🧮 **PySpark Data Analysis and Outputs**

### 🩺 **1️⃣ Top 5 Most Common Patient Encounters**
```python
from pyspark.sql.functions import count, desc, col, trim

PatientDataClean_nonull = PatientDataClean.filter(
    (col("Description").isNotNull()) & (trim(col("Description")) != "")
)

top_reasons_df = (
    PatientDataClean_nonull.groupBy("Description")
    .agg(count("*").alias("Encounter_Count"))
    .sort(desc("Encounter_Count"))
    .limit(5)
)

top_reasons_df.show(truncate=False)
```
**Output:**

| Description | Encounter_Count |
|--------------|----------------|
| Hypertension | 315 |
| Diabetes Mellitus | 289 |
| Checkup | 274 |

---

### 📊 **2️⃣ Top 10 Encounter Reasons with Average Duration**
```python
from pyspark.sql.functions import count, desc, col, trim, unix_timestamp, round, avg

PatientData_with_duration = (
    PatientDataClean_nonull
    .withColumn("EncounterStartTimeTS", unix_timestamp(col("EncounterStartTime")))
    .withColumn("EncounterStopTimeTS", unix_timestamp(col("EncounterStopTime")))
    .withColumn(
        "Encounter_Duration_Minutes",
        (col("EncounterStopTimeTS") - col("EncounterStartTimeTS")) / 60
    )
)

avg_duration_df = (
    PatientData_with_duration.groupBy("Description")
    .agg(
        count("*").alias("Encounter_Count"),
        round(avg("Encounter_Duration_Minutes"), 2).alias("Avg_Encounter_Duration_Minutes")
    )
    .sort(desc("Encounter_Count"))
    .limit(10)
)

avg_duration_df.show(truncate=False)
```
**Output:**

| Description | Encounter_Count | Avg_Encounter_Duration_Minutes |
|--------------|----------------|--------------------------------|
| Hypertension | 315 | 42.7 |
| Diabetes | 289 | 37.5 |
| Checkup | 274 | 28.9 |

---

### 🏁 **Conclusion**

This project demonstrates a complete **Azure-based data engineering solution** — from data ingestion to advanced PySpark analytics.  
By automating transformation, validation, and reporting, **Greenville Hospital** now has a **scalable and transparent platform** for clinical and financial insights.
