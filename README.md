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

## 🗺️ **Data Architecture Flow**

Data Architecture Flow.jpg

---

## 🧮 **PySpark Data Analysis and Outputs**

---

### 🩺 **1️⃣ What are the top 5 most common patient encounters Greenville Hospital receives**

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
![](1cd2056c-29e9-43d2-ab19-b1d1a8327133.png)

---

### 📊 **2️⃣ What is the average duration of the top 10 most common patient encounters Greenville Hospital receives**

```python
from pyspark.sql.functions import count, desc, col, trim, avg, unix_timestamp

PatientDataClean_nonull = PatientDataClean.filter(
    (col("Description").isNotNull()) & (trim(col("Description")) != "")
)

top_reasons_df = (
    PatientDataClean_nonull.groupBy("Description")
    .agg(count("*").alias("Encounter_Count"))
    .sort(desc("Encounter_Count"))
    .limit(10)
)

PatientData_with_duration = (
    PatientDataClean_nonull
    .withColumn("EncounterStartTimeTS", unix_timestamp(col("EncounterStartTime")))
    .withColumn("EncounterStopTimeTS", unix_timestamp(col("EncounterStopTime")))
    .withColumn("Encounter_Duration_Minutes", 
                (col("EncounterStopTimeTS") - col("EncounterStartTimeTS")) / 60)
)

PatientData_top10 = PatientData_with_duration.join(
    top_reasons_df.select("Description"),
    on="Description",
    how="inner"
)

avg_duration_df = (
    PatientData_top10.groupBy("Description")
    .agg(
        count("*").alias("Encounter_Count"),
        avg(col("Encounter_Duration_Minutes")).alias("Avg_Encounter_Duration_Minutes")
    )
    .sort(desc("Encounter_Count"))
)

avg_duration_df.show(truncate=False)
```

**Output:**  
![](c5e825eb-c7dd-44f5-96af-a456217a2004.png)

---

### 🏥 **3️⃣ Retrieve each encounter classification by the most # count of visits to the least**

```python
from pyspark.sql.functions import col, count, trim, desc

noinsurance_df = PatientDataClean.filter(
    (col("InsuranceName").isNotNull()) &
    (trim(col("InsuranceName")).isin("Noinsurance", "NOINSURANCE", "noinsurance")) &
    (col("EncounterClass").isNotNull()) &
    (trim(col("EncounterClass")) != "")
)

encounterclass_counts_df = (
    noinsurance_df.groupBy("EncounterClass")
    .agg(count("*").alias("Visit_Count"))
    .sort(desc("Visit_Count"))
)

encounterclass_counts_df.show(truncate=False)
```

**Output:**  
![](678ca530-3f37-42fb-8ccd-afdd0abd8168.png)

---

### 💳 **4️⃣ Find the top 3 payers by total payments**

```python
from pyspark.sql.functions import col, sum as spark_sum, desc, trim, format_number, concat, lit

PatientDataClean_nonull_payers = PatientDataClean.filter(
    (col("InsuranceName").isNotNull()) &
    (trim(col("InsuranceName")) != "") &
    (trim(col("InsuranceName")) != "Noinsurance")
)

payers_total_df = (
    PatientDataClean_nonull_payers.groupBy("InsuranceName")
    .agg(spark_sum(col("Total_Claim_Cost")).alias("Total_Payments"))
    .sort(desc("Total_Payments"))
    .limit(3)
)

payers_formatted_df = payers_total_df.select(
    col("InsuranceName"),
    concat(lit("$"), format_number(col("Total_Payments"), 2)).alias("Total_Payments_USD")
)

payers_formatted_df.show(truncate=False)
```

**Output:**  
![](dc06f32e-f6fc-460d-a4e1-516310ff42b4.png)

---

### 💵 **5️⃣ What is the total amount of medical expenses not paid by insurance**

```python
from pyspark.sql.functions import col, sum as spark_sum, desc, trim, format_number, concat, lit

noinsurance_df = PatientDataClean.filter(
    (col("InsuranceName").isNotNull()) &
    (trim(col("InsuranceName")).isin("Noinsurance", "NOINSURANCE", "noinsurance"))
)

noinsurance_total_df = (
    noinsurance_df.groupBy("InsuranceName")
    .agg(spark_sum(col("Total_Claim_Cost")).alias("Total_Payments"))
    .sort(desc("Total_Payments"))
)

noinsurance_formatted_df = noinsurance_total_df.select(
    col("InsuranceName"),
    concat(lit("$"), format_number(col("Total_Payments"), 2)).alias("Total_Payments_USD")
)

noinsurance_formatted_df.show(truncate=False)
```

**Output:**  
![](194c6e32-227f-4a07-9ead-140eeabd497b.png)

---

### 👥 **6️⃣ What demographic pays the most in medical expenses**

```python
from pyspark.sql.functions import (
    col, sum as spark_sum, trim, current_date, datediff, floor,
    concat, lit, format_number
)

noinsurance_df = PatientDataClean.filter(
    (col("InsuranceName").isNotNull()) &
    (trim(col("InsuranceName")).isin("Noinsurance", "NOINSURANCE", "noinsurance"))
)

noinsurance_with_age = noinsurance_df.withColumn(
    "Patient_Age",
    floor(datediff(current_date(), col("PatientBirthday")) / 365.25)
)

noinsurance_by_demo = (
    noinsurance_with_age.groupBy(
        "Patient_Age",
        "PatientMarital",
        "PatientRace",
        "PatientEthnicity",
        "PatientGender"
    )
    .agg(spark_sum(col("Total_Claim_Cost")).alias("Total_Noinsurance_Payments"))
    .sort(col("Total_Noinsurance_Payments").desc())
)

noinsurance_formatted = noinsurance_by_demo.select(
    "Patient_Age", "PatientMarital", "PatientRace", "PatientEthnicity", "PatientGender",
    concat(lit("$"), format_number(col("Total_Noinsurance_Payments"), 2)).alias("Total_Payments_USD")
)

noinsurance_formatted.show(truncate=False)
```

**Output:**  
![](000746bc-0162-4392-81a2-5db173df888f.png)

---

### 🧬 **7️⃣ For each demographic, what procedure is most common?**

```python
from pyspark.sql import Window
from pyspark.sql.functions import (
    col, count, trim, current_date, datediff, floor,
    row_number, desc
)

noinsurance_df = PatientDataClean.filter(
    (col("InsuranceName").isNotNull()) &
    (trim(col("InsuranceName")).isin("Noinsurance", "NOINSURANCE", "noinsurance")) &
    (col("ProcedureDescription").isNotNull()) &
    (trim(col("ProcedureDescription")) != "")
)

noinsurance_with_age = noinsurance_df.withColumn(
    "Patient_Age",
    floor(datediff(current_date(), col("PatientBirthday")) / 365.25)
)

agg_df = (
    noinsurance_with_age.groupBy(
        "Patient_Age", "PatientMarital", "PatientRace", "PatientEthnicity", "PatientGender", "ProcedureDescription"
    )
    .agg(count("*").alias("Visit_Count"))
)

window_spec = Window.partitionBy(
    "Patient_Age", "PatientMarital", "PatientRace", "PatientEthnicity", "PatientGender"
).orderBy(desc("Visit_Count"))

ranked_df = agg_df.withColumn("rank", row_number().over(window_spec))
top_proc_df = ranked_df.filter(col("rank") == 1)

final_df = top_proc_df.select(
    "Patient_Age", "PatientMarital", "PatientRace", "PatientEthnicity", "PatientGender", "ProcedureDescription", "Visit_Count"
).orderBy(col("Visit_Count").desc())

final_df.show(truncate=False)
```

**Output:**  
![](c63e5028-d599-47cf-bdfd-088fb99e8ee0.png)

---

### 📅 **8️⃣ What is the average age of patients who visit the hospital**

```python
from pyspark.sql.functions import col, trim, current_date, datediff, floor, avg

noinsurance_df = PatientDataClean.filter(
    (col("InsuranceName").isNotNull()) &
    (trim(col("InsuranceName")).isin("Noinsurance", "NOINSURANCE", "noinsurance")) &
    (col("ProcedureReasonDescription").isNotNull()) &
    (trim(col("ProcedureReasonDescription")) != "")
)

noinsurance_with_age = noinsurance_df.withColumn(
    "Patient_Age",
    floor(datediff(current_date(), col("PatientBirthday")) / 365.25)
)

avg_age_df = noinsurance_with_age.select(avg(col("Patient_Age")).alias("Average_Age"))
avg_age_df.show()
```

**Output:**  
![](d86f37cf-2f9a-40b2-af98-0f570addb173.png)

---

## ⚠️ **Issues and Challenges Faced**

| Challenge | Description | Resolution / Approach |
|------------|-------------|------------------------|
| **Data Quality** | Missing IDs and invalid timestamps | Used `filter()` and `na.drop()` for targeted cleaning |
| **Timestamp Validation** | Stop times earlier than start times | Flagged and removed invalid durations |
| **Outliers** | Extreme values in claim costs / durations | Detected using IQR method in PySpark |
| **Performance** | Large dataset during aggregation | Used `approxQuantile()` and partition optimization |
| **Special Characters** | Inconsistent text inputs | Cleaned with `trim()` and `regexp_replace()` |

---

## 🚀 **Next Steps / Future Enhancements**

- Integrate with **Power BI Dashboards** for dynamic claim and encounter analytics.  
- Automate **ADF Pipeline Triggers** for daily / weekly refresh.  
- Implement **Data Quality Auditing** with logging and thresholds.  
- Add **Historical Tracking (SCD Type-2)** for longitudinal records.  
- Develop **Predictive Models** for forecasting encounter volumes and claim costs.  
- Strengthen **Data Governance & Security** with PHI masking and RBAC.  

---

## 🏁 **Conclusion**

This project demonstrates a complete **Azure-based data engineering solution** — from data ingestion to advanced PySpark analytics.  
By automating transformation, validation, and reporting, **Greenville Hospital** now has a **scalable and transparent platform** for clinical and financial insights.
