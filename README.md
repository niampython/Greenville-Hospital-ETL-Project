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
🧩 Step 1: Remove Null or Blank Descriptions
from pyspark.sql.functions import count, desc, col, trim

PatientDataClean_nonull = PatientDataClean.filter(
    (col("Description").isNotNull()) & (trim(col("Description")) != "")
)


Output (Sample Result):

Description	Encounter_Count
Hypertension	315
Diabetes	289
Checkup	274
📊 Step 2: Top 10 Encounter Reasons
top_reasons_df = (
    PatientDataClean_nonull.groupBy("Description")
    .agg(count("*").alias("Encounter_Count"))
    .sort(desc("Encounter_Count"))
    .limit(10)
)
top_reasons_df.show()


Output (Top 10 Encounter Reasons):

Description	Encounter_Count
Hypertension	315
Diabetes	289
Checkup	274
...	...
⏱ Step 3: Calculate Encounter Durations
from pyspark.sql.functions import unix_timestamp

PatientData_with_duration = (
    PatientDataClean_nonull
    .withColumn("EncounterStartTimeTS", unix_timestamp(col("EncounterStartTime")))
    .withColumn("EncounterStopTimeTS", unix_timestamp(col("EncounterStopTime")))
    .withColumn(
        "Encounter_Duration_Minutes", 
        (col("EncounterStopTimeTS") - col("EncounterStartTimeTS")) / 60
    )
)


Output (Encounter Duration in Minutes):

EncounterStartTime	EncounterStopTime	Encounter_Duration_Minutes
2025-01-10 10:00:00	2025-01-10 10:45:00	45
2025-02-15 08:30:00	2025-02-15 09:10:00	40

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
By automating transformation, validation, and analytics, Greenville Hospital now has a scalable and transparent platform for clinical and financial insights.
By automating transformation, validation, and analytics, Greenville Hospital now has a scalable and transparent platform for clinical and financial insights.

