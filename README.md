# Incremental-Data-Injection-Pipeline-SQL-to-ADLS-Gen2-Project
This is an end-to-end ADF pipeline that extracts data from an Azure SQL Database, applies incremental loading using Watermark column, and loads it into ADLS Gen2 as Parquet files.

Key features:
* Parameterized pipelines for multiple tables
* Incremental loading using watermark column(LastModifiedDate)
* Dynamic folder and file naming
* Integration with Logic Apps for alerting

<img width="413" height="213" alt="Image" src="https://github.com/user-attachments/assets/b12df437-ed07-4a99-b9d1-19c8f154df9b" />

### Azure SQL → Lookup (LastModifiedDate) → ForEach → Copy → If Condition → ADLS → Logic Apps

## Pipeline Activities
#### This pipeline uses the following activities:
* Lookup (LastModifiedDate) – Get last processed value
* ForEach – Loops through each table using ForEach for dynamic ingestion
* Set Variable (current) – Store current timestamp
* Copy (AzureSql_to_Adlsgen2) – Writes incremental data to ADLS Gen2 in Parquet format
* If Condition – Check if data is loaded
* Delete (Delete_Empty_file) – Remove empty file
* Script (max_date) – Get latest value from source 
* Copy (update_LastModifiedDate) – Updates the watermark when new data is found
* Web Activity – Send pipeline status

  


