🏥 Greenville Hospital Patient Records Data Engineering Project
📘 Project Overview

Greenville Hospital stores unclean patient record CSV data in Azure Blob Storage.
This project designs a complete Azure Data Engineering pipeline to ingest, clean, transform, and analyze that data using:

Azure Data Factory (ADF) for ETL orchestration

Azure Blob Storage for raw and clean data zones

Azure Databricks (PySpark) for data validation and analytics

Azure SQL Database for final structured storage

💼 Business Purpose

Greenville Hospital wanted to:

Understand patient encounter patterns and insurance payer performance

Reduce reporting latency caused by messy raw CSV data from APIs

Centralize data into Azure SQL for BI tools (Power BI, Tableau)

Identify high-cost demographics, average encounter durations, and uninsured financial exposure

By modernizing its data pipeline on Azure, Greenville Hospital gained automated, reliable insights into patient operations and billing.

⚙️ ETL Breakdown
Phase	Tool	Key Steps
Extract	Azure Data Factory	Ingest raw CSVs from Blob Storage (raw/ container)
Transform	ADF Data Flow	Select, derive, clean special chars, cast types, standardize dates, join datasets
Load	ADF → Azure Blob Storage	Write cleaned data to clean/ container
Analyze	Azure Databricks (PySpark)	Validate schema + outliers, perform analytics, load final results to Azure SQL Database
🧱 Data Flow Architecture

Workflow Summary

Unclean Data Storage → Azure Blob Storage (raw/)

ADF Pipeline → Cleans, standardizes, and joins CSVs

Clean Data Storage → Azure Blob Storage (clean/)

Azure Databricks → Performs quality checks and analytics

Azure SQL Database → Final clean dataset loaded into [dbo].[Patient_Medical_Records]

🧩 Data Flow in Azure Data Factory
Step	Transformation	Purpose
Source	Reads CSV files from unclean Blob Storage	Raw input ingestion
Select	Chooses only relevant patient columns	Reduces schema complexity
Derived Column	Fixes column names, types, and formats	Cleanses data
Join	Combines multiple related CSVs	Creates unified dataset
Sink	Writes clean data to new Blob Storage	Outputs transformed dataset
🧪 PySpark Analysis & Results

Below are the analysis questions and corresponding PySpark code and outputs executed in Azure Databricks.

1️⃣ Top 5 Most Common Patient Encounters
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


2️⃣ Average Duration of Top 10 Encounters
from pyspark.sql.functions import unix_timestamp, avg
PatientData_with_duration = (
    PatientDataClean_nonull
    .withColumn("EncounterStartTS", unix_timestamp(col("EncounterStartTime")))
    .withColumn("EncounterStopTS", unix_timestamp(col("EncounterStopTime")))
    .withColumn("Encounter_Duration_Minutes",
                (col("EncounterStopTS") - col("EncounterStartTS")) / 60)
)
avg_duration_df = (
    PatientData_with_duration.groupBy("Description")
    .agg(count("*").alias("Encounter_Count"),
         avg("Encounter_Duration_Minutes").alias("Avg_Encounter_Duration_Minutes"))
    .sort(desc("Encounter_Count"))
    .limit(10)
)
avg_duration_df.show(truncate=False)


3️⃣ Encounter Classification by Visit Count (No Insurance)
from pyspark.sql.functions import col, count, trim, desc
noinsurance_df = PatientDataClean.filter(
    (col("InsuranceName").isNotNull()) &
    (trim(col("InsuranceName")).isin("Noinsurance", "NOINSURANCE", "noinsurance")) &
    (col("EncounterClass").isNotNull()) & (trim(col("EncounterClass")) != "")
)
encounterclass_counts_df = (
    noinsurance_df.groupBy("EncounterClass")
    .agg(count("*").alias("Visit_Count"))
    .sort(desc("Visit_Count"))
)
encounterclass_counts_df.show(truncate=False)


4️⃣ Top 3 Payers by Total Payments
from pyspark.sql.functions import sum as spark_sum, desc, trim, concat, lit, format_number
PatientDataClean_nonull_payers = PatientDataClean.filter(
    (col("InsuranceName").isNotNull()) & (trim(col("InsuranceName")) != "") &
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


5️⃣ Total Uninsured Medical Expenses
noinsurance_df = PatientDataClean.filter(
    (col("InsuranceName").isNotNull()) &
    (trim(col("InsuranceName")).isin("Noinsurance", "NOINSURANCE", "noinsurance"))
)
noinsurance_total_df = (
    noinsurance_df.groupBy("InsuranceName")
    .agg(spark_sum(col("Total_Claim_Cost")).alias("Total_Payments"))
)
noinsurance_formatted_df = noinsurance_total_df.select(
    col("InsuranceName"),
    concat(lit("$"), format_number(col("Total_Payments"), 2)).alias("Total_Payments_USD")
)
noinsurance_formatted_df.show(truncate=False)


6️⃣ Demographics Paying Most ( No Insurance )
from pyspark.sql.functions import floor, current_date, datediff
noinsurance_with_age = noinsurance_df.withColumn(
    "Patient_Age",
    floor(datediff(current_date(), col("PatientBirthday")) / 365.25)
)
noinsurance_by_demo = (
    noinsurance_with_age.groupBy(
        "Patient_Age","PatientMarital","PatientRace","PatientEthnicity","PatientGender"
    )
    .agg(spark_sum(col("Total_Claim_Cost")).alias("Total_Noinsurance_Payments"))
    .sort(col("Total_Noinsurance_Payments").desc())
)


7️⃣ Most Common Procedure per Demographic
from pyspark.sql import Window
from pyspark.sql.functions import row_number, desc
agg_df = noinsurance_with_age.groupBy(
    "Patient_Age","PatientMarital","PatientRace","PatientEthnicity","PatientGender","ProcedureDescription"
).agg(count("*").alias("Visit_Count"))
window_spec = Window.partitionBy(
    "Patient_Age","PatientMarital","PatientRace","PatientEthnicity","PatientGender"
).orderBy(desc("Visit_Count"))
ranked_df = agg_df.withColumn("rank", row_number().over(window_spec))
top_proc_df = ranked_df.filter(col("rank") == 1)
top_proc_df.show(truncate=False)


8️⃣ Average Age of Uninsured Patients
from pyspark.sql.functions import avg
avg_age_df = noinsurance_with_age.select(avg(col("Patient_Age")).alias("Average_Age"))
avg_age_df.show()


🧾 Azure SQL Database Verification

Clean data successfully loaded into:
Server: medical-records.database.windows.net
Database: Patient_Records
Table: [dbo].[Patient_Medical_Records]

SELECT TOP (1000)
 [PatientID],[PatientFirstName],[PatientLastName],
 [PatientCity],[PatientState],
 [EncounterStartTime],[EncounterStopTime],
 [ProcedureDescription],[InsuranceName],[Total_Claim_Cost]
FROM [Patient_Records].[dbo].[Patient_Medical_Records];


📊 Insights Summary
Metric	Key Finding
Most Common Encounter	Encounter for Problem Procedure
Longest Encounter Type	Problem Procedure (~102 min avg)
Top Payers	UnitedHealthcare, Medicare, Humana
Uninsured Cost	$10.9 million
Average Age (Uninsured)	89.5 years
Frequent Encounter Classes	Ambulatory, Outpatient, UrgentCare

🪲 Issues and Challenges Faced
Issue	Description	Solution
1. Databricks → SQL Authentication Failure	Encountered “Login failed for user <token-identified principal>” during AAD authentication.	Created SQL login databricks_loader with db_owner privileges and switched to username/password auth.
2. ADF Dataflow Column Type Errors	Columns mismatched due to inconsistent source schema.	Used Derived Column transformations to explicitly cast types.
3. Special Characters in Names (O’Hara, García)	Non-ASCII characters appeared in patient name columns.	Applied regex-based cleaning in ADF and Databricks with regexp_replace().
4. Databricks IP Firewall Blocked	Could not connect to SQL Server from Databricks.	Added Databricks public IP (172.202.17.203) to SQL Server firewall and enabled “Allow Azure Services”.
5. Missing Date or ID Fields	Null IDs and DOBs caused join mismatches.	Filtered invalid rows and logged record counts before and after cleaning.

✅ Next Steps:

Add this README and screenshots to your project root.

Push to GitHub (Greenville-Hospital-ETL-Project).

Optionally include your ADF JSON exports under /adf_pipelines/.

Would you like me to generate a ready-to-upload folder ZIP (with this README and placeholder directories already structured) so you can drag-drop it into VS Code or GitHub Desktop?