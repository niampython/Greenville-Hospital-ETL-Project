🏥 Greenville Hospital Data Engineering & Analytics Project
🧭 Project Overview

The Greenville Hospital ETL & Analytics Project is an end-to-end healthcare data engineering pipeline built to extract, clean, transform, and load patient encounter data from hospital systems into an analytics-ready format.

This project automates data validation, outlier detection, timestamp consistency checks, and business intelligence delivery using Azure Data Factory, Azure Databricks (PySpark), and Azure Data Lake Storage.

It ensures data quality and transparency across hospital operations — enabling analysts, finance teams, and administrators to draw meaningful insights on patient encounters, procedure costs, and payer coverage.

Core technologies:

Azure Data Factory (ADF) – Pipeline orchestration for ingestion, cleaning, and transformation

Azure Databricks (PySpark) – Advanced data processing, anomaly detection, and analytics

Azure Data Lake Storage (ADLS Gen2) – Centralized data lake for raw, cleaned, and curated zones

Power BI / Tableau – Visualization of cost, coverage, and encounter duration trends

Loguru / Python – Logging and monitoring of ETL operations

🎯 Business Purpose

The healthcare sector produces vast amounts of transactional and clinical data that often contain inconsistencies — such as missing patient IDs, negative timestamps, or abnormal costs.
The business need was to create an automated, scalable data pipeline that:

Cleanses and validates patient encounter records

Detects and flags outliers in claim costs, coverage amounts, and encounter durations

Ensures timestamp consistency between procedure and encounter start/stop times

Delivers trusted, curated datasets for financial and operational reporting

Value Delivered:

⏱️ Reduced manual data-cleaning time by automating ETL validation

📊 Improved accuracy in financial and operational reports

🏥 Enabled clinical insights into procedure efficiency and payer performance

🗺️ Data Architecture Flow

Below is the visual architecture flow diagram representing the end-to-end ETL process from ingestion to analysis:

Explanation of Flow:

Uncleaned Patient Records (Azure Blob Storage)
Raw patient encounter data is ingested from multiple sources (CSV, API, or EHR exports) and stored in the raw container of Azure Blob Storage.

Transform Unclean Data (Azure Data Factory)
Azure Data Factory orchestrates transformation pipelines to:

Standardize schemas

Normalize formats

Handle nulls and data type mismatches

Clean invalid timestamps and special characters

Load Clean Data (Azure Blob Storage)
Cleaned and validated patient encounter datasets are stored in the curated container, ready for analysis.

Analysis (Azure Databricks)
Azure Databricks connects to the curated data to:

Detect outliers in costs and coverage using IQR and z-score methods

Validate duration consistency for encounter and procedure times

Aggregate insights such as top payers, procedure cost trends, and claim distributions

⚙️ ETL Workflow Summary

The ETL process is organized into modular stages for scalability and clarity:

Stage	Component	Description
Extract	Azure Data Factory Pipelines	Pulls raw hospital encounter data from APIs or file drops into Blob Storage
Transform	Azure Data Factory & PySpark (Databricks)	Cleans nulls, fixes invalid timestamps, detects outliers, computes durations
Load	Azure Data Lake / Azure SQL	Loads curated data into ADLS Gen2 or SQL Server for analytics
Validate & Log	Python + Loguru	Tracks each ETL run, logging invalid, missing, or outlier records
Visualize	Power BI / Tableau	Displays KPIs such as average claim cost, encounter duration, payer coverage ratios

🧮 PySpark Data Analysis and Insights
🩺 1. What are the top 5 most common patient encounters Greenville Hospital receives?
from pyspark.sql.functions import count, desc, col, trim

# Filter out rows where Description is null or blank
PatientDataClean_nonull = PatientDataClean.filter(
    (col("Description").isNotNull()) & (trim(col("Description")) != "")
)

# Group, count, and sort
top_reasons_df = (
    PatientDataClean_nonull.groupBy("Description")
    .agg(count("*").alias("Encounter_Count"))
    .sort(desc("Encounter_Count"))
    .limit(5)
)

# Show full text in columns (no truncation)
top_reasons_df.show(truncate=False)


Output:

📊 2. What are the top 10 patient encounter reasons with average encounter duration?
from pyspark.sql.functions import count, desc, col, trim, unix_timestamp

# Convert start/stop times and calculate duration
PatientData_with_duration = (
    PatientDataClean_nonull
    .withColumn("EncounterStartTimeTS", unix_timestamp(col("EncounterStartTime")))
    .withColumn("EncounterStopTimeTS", unix_timestamp(col("EncounterStopTime")))
    .withColumn("Encounter_Duration_Minutes", 
                (col("EncounterStopTimeTS") - col("EncounterStartTimeTS")) / 60)
)

# Aggregate encounter count and average duration
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


Output:

🏥 3. Which encounter types are most common (Encounter Class distribution)?
from pyspark.sql.functions import count, desc

encounterclass_counts_df = (
    PatientDataClean.groupBy("EncounterClass")
    .agg(count("*").alias("Visit_Count"))
    .sort(desc("Visit_Count"))
)

encounterclass_counts_df.show(truncate=False)


Output:

💰 4. Which insurance payers cover the most expenses?
from pyspark.sql.functions import col, sum as spark_sum, desc, concat, lit, format_number

# Group and sum total payments by insurance
payers_total_df = (
    PatientDataClean.groupBy("InsuranceName")
    .agg(spark_sum(col("Total_Claim_Cost")).alias("Total_Payments"))
    .sort(desc("Total_Payments"))
)

# Format results as currency
payers_formatted_df = payers_total_df.select(
    "InsuranceName",
    concat(lit("$"), format_number(col("Total_Payments"), 2)).alias("Total_Payments_USD")
)

payers_formatted_df.show(truncate=False)


Output:

💵 5. What is the total amount of medical expenses not paid by insurance?
from pyspark.sql.functions import col, sum as spark_sum, trim, desc, concat, lit, format_number

# Filter 'Noinsurance' records (case-insensitive)
noinsurance_df = PatientDataClean.filter(
    (col("InsuranceName").isNotNull()) &
    (trim(col("InsuranceName")).isin("Noinsurance", "NOINSURANCE", "noinsurance"))
)

# Aggregate and format
noinsurance_total_df = (
    noinsurance_df.groupBy("InsuranceName")
    .agg(spark_sum(col("Total_Claim_Cost")).alias("Total_Payments"))
    .sort(desc("Total_Payments"))
)

noinsurance_formatted_df = noinsurance_total_df.select(
    "InsuranceName",
    concat(lit("$"), format_number(col("Total_Payments"), 2)).alias("Total_Payments_USD")
)

noinsurance_formatted_df.show(truncate=False)


Output:

👥 6. What demographic pays the most in medical expenses?
from pyspark.sql.functions import (
    col, sum as spark_sum, trim, current_date, datediff, floor,
    concat, lit, format_number
)

# Filter for Noinsurance and calculate age
noinsurance_df = PatientDataClean.filter(
    (col("InsuranceName").isNotNull()) &
    (trim(col("InsuranceName")).isin("Noinsurance", "NOINSURANCE", "noinsurance"))
)

noinsurance_with_age = noinsurance_df.withColumn(
    "Patient_Age",
    floor(datediff(current_date(), col("PatientBirthday")) / 365.25)
)

# Group and aggregate by demographics
noinsurance_by_demo = (
    noinsurance_with_age.groupBy(
        "Patient_Age", "PatientMarital", "PatientRace", "PatientEthnicity", "PatientGender"
    )
    .agg(spark_sum(col("Total_Claim_Cost")).alias("Total_Noinsurance_Payments"))
    .sort(col("Total_Noinsurance_Payments").desc())
)

# Format for display
noinsurance_formatted = noinsurance_by_demo.select(
    "Patient_Age", "PatientMarital", "PatientRace", "PatientEthnicity", "PatientGender",
    concat(lit("$"), format_number(col("Total_Noinsurance_Payments"), 2)).alias("Total_Payments_USD")
)

noinsurance_formatted.show(truncate=False)


Output:

🧠 7. For each demographic, what procedure is most common?
from pyspark.sql import Window
from pyspark.sql.functions import col, count, trim, current_date, datediff, floor, row_number, desc

# Filter and calculate age
noinsurance_df = PatientDataClean.filter(
    (col("InsuranceName").isNotNull()) &
    (trim(col("InsuranceName")).isin("Noinsurance", "NOINSURANCE", "noinsurance")) &
    (col("ProcedureDescription").isNotNull()) &
    (trim(col("ProcedureDescription")) != "")
)

noinsurance_with_age = noinsurance_df.withColumn(
    "Patient_Age", floor(datediff(current_date(), col("PatientBirthday")) / 365.25)
)

# Group and rank
agg_df = noinsurance_with_age.groupBy(
    "Patient_Age", "PatientMarital", "PatientRace", "PatientEthnicity", "PatientGender", "ProcedureDescription"
).agg(count("*").alias("Visit_Count"))

window_spec = Window.partitionBy(
    "Patient_Age", "PatientMarital", "PatientRace", "PatientEthnicity", "PatientGender"
).orderBy(desc("Visit_Count"))

ranked_df = agg_df.withColumn("rank", row_number().over(window_spec))
top_proc_df = ranked_df.filter(col("rank") == 1)
final_df = top_proc_df.select(
    "Patient_Age", "PatientMarital", "PatientRace", "PatientEthnicity", "PatientGender", "ProcedureDescription", "Visit_Count"
).orderBy(col("Visit_Count").desc())

final_df.show(truncate=False)


Output:

📅 8. What is the average age of patients who visit the hospital?
from pyspark.sql.functions import col, trim, current_date, datediff, floor, avg

# Filter and calculate age
noinsurance_df = PatientDataClean.filter(
    (col("InsuranceName").isNotNull()) &
    (trim(col("InsuranceName")).isin("Noinsurance", "NOINSURANCE", "noinsurance")) &
    (col("ProcedureReasonDescription").isNotNull()) &
    (trim(col("ProcedureReasonDescription")) != "")
)

noinsurance_with_age = noinsurance_df.withColumn(
    "Patient_Age", floor(datediff(current_date(), col("PatientBirthday")) / 365.25)
)

avg_age_df = noinsurance_with_age.select(avg(col("Patient_Age")).alias("Average_Age"))
avg_age_df.show()


Output:


📈 Insights Summary, Issues & Challenges, and Next Steps
📊 Insights Summary

After performing all ETL transformations and PySpark analyses, several key insights were derived from Greenville Hospital’s patient encounter data:

🏥 1. Common Encounter Trends

The most frequent encounter reasons were:

Encounter for Problem Procedure (14,446 visits)

Urgent Care Clinic Procedure (1,132 visits)

Check-Up Procedure (1,130 visits)

These indicate that problem-related and follow-up visits dominate hospital workflows, emphasizing the importance of preventive care and chronic condition management.

⏱️ 2. Encounter Duration Patterns

Average encounter durations varied widely:

Problem Procedures averaged ~102 minutes, while Telemedicine Consultations averaged only ~13 minutes.

This suggests that in-person procedures are significantly longer, while remote consultations are more time-efficient.

💸 3. Insurance Payer Distribution

UnitedHealthcare covered the largest share of medical payments, totaling $17.28M, followed by Medicare ($3.49M) and Humana ($124K).

The No Insurance category accounted for $10.91M, showing a major opportunity for policy or financial support programs to reduce uninsured treatment costs.

👥 4. Demographic Insights

Patients without insurance were predominantly:

White and Non-Hispanic, though some Hispanic and Black populations were represented.

Male patients contributed slightly more to total uninsured costs.

The average patient age among uninsured visitors was approximately 89.5 years, suggesting higher medical expenses are correlated with elderly demographics.

🧬 5. Most Common Procedures by Demographic

For older demographics, renal dialysis and chemotherapy were among the most frequent procedures.

For middle-aged adults, preventive screenings and fall risk assessments were common.

This reflects age-driven medical service utilization patterns.

⚠️ Issues and Challenges Faced

During the ETL and analytical development, several challenges were encountered:

Challenge	Description	Resolution / Approach
Data Quality	Null or missing values in key columns (e.g., PatientID, ProcedureStartTime, EncounterStopTime)	Used filter() and na.drop() for targeted column cleaning
Invalid Timestamps	Stop times earlier than start times for some records	Created validation filters to flag and drop invalid durations
Outlier Detection	Extremely high claim costs and procedure durations	Applied IQR method in PySpark to detect and isolate anomalies
Formatting Issues	Special characters and inconsistent naming in source data	Used trim(), regexp_replace(), and normalization logic
Performance	Large dataset size causing slow aggregations	Optimized Spark transformations and used approxQuantile for statistical summaries
🚀 Next Steps and Future Enhancements

To further enhance the Greenville Hospital Data Engineering pipeline and its analytical capabilities, the following next steps are recommended:

📦 Integrate with Azure SQL & Power BI:
Load the curated output tables to Azure SQL Database and connect Power BI dashboards for live KPI tracking (e.g., patient load, claim cost trends).

🔁 Automate Pipeline Scheduling:
Set up ADF triggers to run extraction and transformation processes daily or weekly for continuous data refresh.

🧩 Add Data Quality Monitoring Layer:
Implement data validation checks and logging with thresholds (e.g., invalid timestamp count, null ratios).

📅 Implement Historical Tracking:
Add a slowly changing dimension (SCD) layer for patient and insurance data to track longitudinal trends.

📉 Predictive Analytics:
Use Databricks ML or Azure Machine Learning to forecast procedure volumes, patient inflow, and cost predictions.

🔐 Enhance Data Governance:
Introduce RBAC (Role-Based Access Control) and masking for sensitive PHI columns during transformation.