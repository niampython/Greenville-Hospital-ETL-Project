# 🏥 **Greenville Hospital Data Engineering ETL Project**

---

## 💡 **Project Overview**

The **Greenville Hospital ETL Project** automates the ingestion, cleaning, transformation, and analysis of patient medical data.  
The hospital stores **unclean CSV data** from an API into an **Azure Storage Blob**.  
An **Azure Data Factory (ADF) pipeline** extracts this data, cleans and standardizes it using a **Data Flow**, and then loads the cleaned data into a **separate blob container**.  
The cleaned data is later connected to **Azure Databricks** for data quality checks, analytics, and insights — before being stored in **Azure SQL Database** for long-term storage and BI use.

---

## ⚙️ **ETL Workflow Breakdown**

| ETL Phase | Tool Used | Description |
|------------|------------|-------------|
| **Extract** | Azure Data Factory | Ingests unclean patient CSV data from Azure Blob Storage. |
| **Transform** | ADF Data Flow | Applies column renaming, type conversion, special character removal, and joins multiple datasets into one unified table. |
| **Load** | Azure Blob Storage (clean container) | Stores the transformed data for analytical use. |
| **Analyze** | Azure Databricks (PySpark) | Performs data validation, outlier detection, and demographic analysis. |
| **Store** | Azure SQL Database | Final clean dataset is loaded into `[dbo].[Patient_Medical_Records]` for BI & reporting. |

---

## 🎯 **Business Purpose**

The purpose of this project is to help **Greenville Hospital** gain better insights into:
- The most common patient encounters and their average durations.
- The demographics of patients and their insurance coverage.
- Total medical costs by insurance payer and uninsured patients.
- Average age and encounter patterns of visitors.

By automating this data pipeline, Greenville Hospital can improve **billing accuracy**, **resource allocation**, and **patient service efficiency**.

---

## 🧩 **Data Flow Architecture (ADF Overview)**

### Azure Data Factory Data Flow

**Diagram Highlights:**
- Data sources: patient info, encounters, payers, and procedures CSVs.
- Transformations:  
  - Rename columns  
  - Convert data types  
  - Remove special characters  
  - Standardize date/time formats  
  - Join datasets
- Output sink: clean dataset stored in a dedicated Azure Blob container.

📸 *See screenshots included in `/Azure` folder:*
- `PatientRecords DataFlow Overview.jpg`
- `Update-ColumnName-Format.jpg`
- `DataFlow Sink Datasource.jpg`

---

## 🔍 **Data Quality and Analysis (Azure Databricks)**

The following sections include **PySpark code** and their corresponding **outputs** from analysis performed on the cleaned data.

---

### 🩺 **1. Top 5 Most Common Patient Encounters**

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
Output:

sql
Copy code
+----------------------------------------+---------------+
|Description                             |Encounter_Count|
+----------------------------------------+---------------+
|Encounter For Problem Procedure         |1446           |
|Urgent Care Clinic Procedure            |1132           |
|Encounter For Check Up Procedure        |1130           |
|General Examination Of Patient Procedure|981            |
|Telemedicine Consultation With Patient  |680            |
+----------------------------------------+---------------+
🕒 2. Average Duration of Top 10 Encounters
python
Copy code
from pyspark.sql.functions import unix_timestamp

PatientData_with_duration = (
    PatientDataClean_nonull
    .withColumn("EncounterStartTimeTS", unix_timestamp(col("EncounterStartTime")))
    .withColumn("EncounterStopTimeTS", unix_timestamp(col("EncounterStopTime")))
    .withColumn("Encounter_Duration_Minutes",
                (col("EncounterStopTimeTS") - col("EncounterStartTimeTS")) / 60)
)

avg_duration_df = (
    PatientData_with_duration.groupBy("Description")
    .agg(count("*").alias("Encounter_Count"),
         avg(col("Encounter_Duration_Minutes")).alias("Avg_Encounter_Duration_Minutes"))
    .sort(desc("Encounter_Count"))
    .limit(10)
)

avg_duration_df.show(truncate=False)
Output:

sql
Copy code
+----------------------------------------+---------------+------------------------------+
|Description                             |Encounter_Count|Avg_Encounter_Duration_Minutes|
+----------------------------------------+---------------+------------------------------+
|Encounter For Problem Procedure         |1446           |102.46                        |
|Urgent Care Clinic Procedure            |1132           |11.99                         |
|Encounter For Check Up Procedure        |1130           |13.89                         |
|General Examination Of Patient Procedure|981            |14.59                         |
|Telemedicine Consultation With Patient  |680            |12.90                         |
+----------------------------------------+---------------+------------------------------+
💰 3. Encounter Classes for Uninsured Patients
python
Copy code
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
Output:

diff
Copy code
+--------------+-----------+
|EncounterClass|Visit_Count|
+--------------+-----------+
|Ambulatory    |1376       |
|Outpatient    |952        |
|Urgentcare    |590        |
|Wellness      |418        |
|Emergency     |211        |
|Inpatient     |137        |
+--------------+-----------+
🏦 4. Top 3 Insurance Payers by Total Payments
python
Copy code
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
Output:

pgsql
Copy code
+----------------+------------------+
|InsuranceName   |Total_Payments_USD|
+----------------+------------------+
|Unitedhealthcare|$17,287,483.78    |
|Medicare        |$3,498,745.36     |
|Humana          |$124,356.94       |
+----------------+------------------+
🧾 5. Total Expenses Not Covered by Insurance
python
Copy code
noinsurance_total_df = (
    PatientDataClean.filter(
        (col("InsuranceName").isNotNull()) &
        (trim(col("InsuranceName")).isin("Noinsurance", "NOINSURANCE", "noinsurance"))
    )
    .groupBy("InsuranceName")
    .agg(spark_sum(col("Total_Claim_Cost")).alias("Total_Payments"))
    .sort(desc("Total_Payments"))
)

noinsurance_formatted_df = noinsurance_total_df.select(
    col("InsuranceName"),
    concat(lit("$"), format_number(col("Total_Payments"), 2)).alias("Total_Payments_USD")
)

noinsurance_formatted_df.show(truncate=False)
Output:

pgsql
Copy code
+-------------+------------------+
|InsuranceName|Total_Payments_USD|
+-------------+------------------+
|Noinsurance  |$10,915,952.23    |
+-------------+------------------+
👥 6. Demographic Groups Paying the Most
python
Copy code
from pyspark.sql.functions import current_date, datediff, floor

noinsurance_with_age = PatientDataClean.filter(
    (col("InsuranceName").isNotNull()) &
    (trim(col("InsuranceName")).isin("Noinsurance", "NOINSURANCE", "noinsurance"))
).withColumn(
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

noinsurance_by_demo.show(truncate=False)
Output:

pgsql
Copy code
+-----------+--------------+-----------+----------------+-------------+------------------+
|Patient_Age|PatientMarital|PatientRace|PatientEthnicity|PatientGender|Total_Payments_USD|
+-----------+--------------+-----------+----------------+-------------+------------------+
|64         |M             |Black      |Hispanic        |F            |$9,555,786.36     |
|102        |S             |White      |Nonhispanic     |M            |$398,277.30       |
|92         |M             |White      |Nonhispanic     |F            |$308,119.68       |
+-----------+--------------+-----------+----------------+-------------+------------------+
🧮 7. Most Common Procedure per Demographic
python
Copy code
from pyspark.sql import Window
from pyspark.sql.functions import count, row_number, desc

agg_df = (
    noinsurance_with_age.groupBy(
        "Patient_Age", "PatientMarital", "PatientRace", "PatientEthnicity", "PatientGender", "ProcedureDescription"
    )
    .agg(count("*").alias("Visit_Count"))
)

window_spec = Window.partitionBy(
    "Patient_Age", "PatientMarital", "PatientRace", "PatientEthnicity", "PatientGender"
).orderBy(desc("Visit_Count"))

top_proc_df = agg_df.withColumn("rank", row_number().over(window_spec)).filter(col("rank") == 1)

top_proc_df.show(truncate=False)
Output:

sql
Copy code
+-----------+--------------+-----------+----------------+-------------+-------------------------------+-----------+
|Patient_Age|PatientMarital|PatientRace|PatientEthnicity|PatientGender|ProcedureDescription            |Visit_Count|
+-----------+--------------+-----------+----------------+-------------+-------------------------------+-----------+
|64         |M             |Black      |Hispanic        |F            |Renal Dialysis Procedure        |2622       |
|94         |M             |White      |Nonhispanic     |F            |Chemotherapy & Radiation Therapy|30         |
+-----------+--------------+-----------+----------------+-------------+-------------------------------+-----------+
📊 8. Average Age of Patients
python
Copy code
from pyspark.sql.functions import avg

avg_age_df = noinsurance_with_age.select(avg(col("Patient_Age")).alias("Average_Age"))
avg_age_df.show()
Output:

diff
Copy code
+-----------+
|Average_Age|
+-----------+
|       89.5|
+-----------+
🗄️ Azure SQL Database Integration
The final cleaned dataset (PatientDataClean) was loaded into Azure SQL Database
in the table [dbo].[Patient_Medical_Records] using the PySpark JDBC connector.

Example Write Command:

python
Copy code
PatientDataClean.write \
    .format("jdbc") \
    .mode("overwrite") \
    .option("url", "jdbc:sqlserver://medical-records.database.windows.net:1433;database=Patient_Records;encrypt=true;trustServerCertificate=false;") \
    .option("dbtable", "dbo.Patient_Medical_Records") \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .option("user", "databricks_loader") \
    .option("password", "<your-password>") \
    .save()
🪲 Issues and Challenges Faced
#	Issue	Description	Solution
1	Databricks → SQL Authentication Failure	Encountered “Login failed for user <token-identified principal>” during AAD authentication.	Created SQL login databricks_loader with db_owner privileges and switched to username/password auth.
2	ADF Dataflow Column Type Errors	Some columns mismatched due to inconsistent source schema.	Used Derived Column transformations to explicitly cast to correct types.
3	Special Characters in Names (e.g., O’Hara, García)	Non-ASCII characters appeared in patient name columns.	Used regex-based cleaning in both ADF and Databricks with PySpark regexp_replace().
4	Databricks IP Firewall Blocked	Could not connect to SQL Server from Databricks.	Added Databricks public outbound IP (172.202.17.203) to SQL Server firewall and enabled “Allow Azure Services”.
5	Missing Date or ID Fields	Null IDs and DOBs caused join mismatches.	Filtered invalid rows and logged record counts before and after cleaning.

🧠 Key Takeaways

Implemented a complete Azure ETL pipeline from ingestion → transformation → load → analysis.

Automated data quality checks in Databricks with PySpark.

Integrated Azure SQL Database for structured data storage.

Built a maintainable, secure, and scalable healthcare data solution.

👤 Author

Niam Dickerson
📧 niam_dataengineer@yahoo.com
💻 GitHub: @niampython
