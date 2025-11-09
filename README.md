🏥 Greenville Hospital Data Engineering & Analytics Project
🧭 Project Overview

The Greenville Hospital ETL & Analytics Project is an end-to-end data engineering pipeline designed to extract, clean, transform, and load patient encounter data from hospital systems into an analytics-ready format.

This project automates:

Data validation

Outlier detection

Timestamp consistency checks

Reporting integration via Power BI/Tableau

It leverages Azure Data Factory, Azure Databricks (PySpark), and Azure Data Lake Storage (ADLS Gen2) to ensure data quality and transparency across the hospital’s operations.

Core Technologies Used

🔹 Azure Data Factory (ADF) – Pipeline orchestration for ingestion and cleaning

🔹 Azure Databricks (PySpark) – Transformation, validation, and analytics

🔹 Azure Data Lake Storage (ADLS Gen2) – Centralized data lake for raw and curated layers

🔹 SQL Server / Azure SQL Database – Analytical storage for BI

🔹 Power BI / Tableau – Visualization and dashboarding

🔹 Loguru – Logging and ETL monitoring

🎯 Business Purpose

Hospitals generate large amounts of encounter and claim data often containing inconsistencies such as:

Missing PatientIDs

Invalid Start/Stop timestamps

Outliers in Total Claim Costs and Payer Coverage

This project ensures high-quality, consistent data for:

💰 Finance teams — monitor claim cost trends

🏥 Operations — understand encounter flow and resource efficiency

📊 Analytics teams — generate reliable KPIs for executive dashboards

Business Outcomes

⏱️ Reduced manual data-cleaning workload

📈 Improved accuracy in financial and operational reports

🧩 Established a foundation for predictive analytics

🗺️ Data Architecture Flow

Flow Explanation

Raw Data (Azure Blob Storage):
Uncleaned patient encounter data from multiple sources (CSV/API/EHR) is ingested into the raw zone.

Transformation (Azure Data Factory):
Data is cleaned, standardized, and normalized through ADF pipelines.

Load Clean Data (Azure Blob Storage):
Validated and structured data stored in the curated layer for analysis.

Analysis (Azure Databricks):
Databricks performs advanced analytics — detecting outliers, validating timestamps, and computing insights.

⚙️ ETL Workflow Summary
Stage	Component	Description
Extract	Azure Data Factory Pipelines	Pulls raw encounter data into ADLS Gen2
Transform	PySpark (Databricks)	Cleans nulls, validates timestamps, detects outliers
Load	Azure Data Lake / SQL	Loads curated data into storage for reporting
Validate & Log	Python + Loguru	Tracks ETL executions and anomalies
Visualize	Power BI / Tableau	Presents claim cost and patient flow metrics
🧮 PySpark Data Analysis and Outputs
🩺 1. Top 5 Most Common Patient Encounters
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


Output:

📊 2. Top 10 Encounter Reasons with Average Duration
from pyspark.sql.functions import count, desc, col, trim, unix_timestamp, round, avg

PatientData_with_duration = (
    PatientDataClean_nonull
    .withColumn("EncounterStartTimeTS", unix_timestamp(col("EncounterStartTime")))
    .withColumn("EncounterStopTimeTS", unix_timestamp(col("EncounterStopTime")))
    .withColumn("Encounter_Duration_Minutes", 
                (col("EncounterStopTimeTS") - col("EncounterStartTimeTS")) / 60)
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


Output:

🏥 3. Encounter Class Distribution
from pyspark.sql.functions import count, desc

encounterclass_counts_df = (
    PatientDataClean.groupBy("EncounterClass")
    .agg(count("*").alias("Visit_Count"))
    .sort(desc("Visit_Count"))
)

encounterclass_counts_df.show(truncate=False)


Output:

💳 4. Insurance Payers with Highest Total Payments
from pyspark.sql.functions import col, sum as spark_sum, desc, concat, lit, format_number

payers_total_df = (
    PatientDataClean.groupBy("InsuranceName")
    .agg(spark_sum(col("Total_Claim_Cost")).alias("Total_Payments"))
    .sort(desc("Total_Payments"))
)

payers_formatted_df = payers_total_df.select(
    "InsuranceName",
    concat(lit("$"), format_number(col("Total_Payments"), 2)).alias("Total_Payments_USD")
)

payers_formatted_df.show(truncate=False)


Output:

💵 5. Total Medical Expenses Not Covered by Insurance
from pyspark.sql.functions import col, sum as spark_sum, trim, desc, concat, lit, format_number

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
    "InsuranceName",
    concat(lit("$"), format_number(col("Total_Payments"), 2)).alias("Total_Payments_USD")
)

noinsurance_formatted_df.show(truncate=False)


Output:

👥 6. Demographics Contributing Most to Medical Expenses
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
        "Patient_Age", "PatientMarital", "PatientRace", "PatientEthnicity", "PatientGender"
    )
    .agg(spark_sum(col("Total_Claim_Cost")).alias("Total_Noinsurance_Payments"))
    .sort(col("Total_Noinsurance_Payments").desc())
)

noinsurance_formatted = noinsurance_by_demo.select(
    "Patient_Age", "PatientMarital", "PatientRace", "PatientEthnicity", "PatientGender",
    concat(lit("$"), format_number(col("Total_Noinsurance_Payments"), 2)).alias("Total_Payments_USD")
)

noinsurance_formatted.show(truncate=False)


Output:

🧬 7. Most Common Procedure per Demographic
from pyspark.sql import Window
from pyspark.sql.functions import col, count, trim, current_date, datediff, floor, row_number, desc

noinsurance_df = PatientDataClean.filter(
    (col("InsuranceName").isNotNull()) &
    (trim(col("InsuranceName")).isin("Noinsurance", "NOINSURANCE", "noinsurance")) &
    (col("ProcedureDescription").isNotNull()) &
    (trim(col("ProcedureDescription")) != "")
)

noinsurance_with_age = noinsurance_df.withColumn(
    "Patient_Age", floor(datediff(current_date(), col("PatientBirthday")) / 365.25)
)

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

📅 8. Average Age of Hospital Patients
from pyspark.sql.functions import col, trim, current_date, datediff, floor, avg

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

📈 Insights, Challenges, and Next Steps
📊 Insights Summary

Problem Procedures and Urgent Care Visits are the most frequent encounter types.

In-person procedures average over 100 minutes, while telemedicine consultations average only 13 minutes.

UnitedHealthcare covers the largest total payments ($17.28M), while No Insurance accounts for $10.9M.

Uninsured patients are predominantly elderly (avg. age ~89.5) and mostly White/Non-Hispanic.

Top uninsured procedures include renal dialysis and chemotherapy for older patients.

⚠️ Issues and Challenges Faced
Challenge	Description	Resolution / Approach
Data Quality	Missing IDs and invalid timestamps	Used filter() and na.drop() for targeted cleaning
Timestamp Validation	Stop times earlier than start times	Flagged and removed invalid durations
Outliers	Extreme values in claim costs/durations	Detected using IQR method in PySpark
Performance	Large dataset during aggregation	Used approxQuantile() and partition optimization
Special Characters	Inconsistent text inputs	Cleaned with trim() and regexp_replace()
🚀 Next Steps / Future Enhancements

Integrate with Power BI Dashboards for dynamic claim and encounter analytics.

Automate ADF Pipeline Triggers for daily/weekly data refresh.

Implement Data Quality Auditing with automated logging and thresholds.

Add Historical Tracking (SCD Type-2) for longitudinal patient records.

Develop Predictive Models for forecasting encounter volumes and claim costs.

Enhance Data Governance & Security with PHI masking and RBAC enforcement.

🏁 Conclusion

This project demonstrates a complete Azure-based data engineering solution — from data ingestion to advanced PySpark analysis.
<<<<<<< HEAD
By automating transformation, validation, and analytics, Greenville Hospital now has a scalable and transparent platform for clinical and financial insights.
=======
By automating transformation, validation, and analytics, Greenville Hospital now has a scalable and transparent platform for clinical and financial insights.
>>>>>>> 69401c0d0e51c9ee23d8a61b7e395d4727a8f79
