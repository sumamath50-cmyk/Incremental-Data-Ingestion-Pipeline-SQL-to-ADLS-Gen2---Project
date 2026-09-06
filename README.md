# Incremental-Data-Ingestion-Pipeline-SQL-to-ADLS-Gen2
This is an end-to-end ADF pipeline that extracts data from an Azure SQL Database, applies incremental loading using Watermark column, and loads it into ADLS Gen2 as Parquet files.

Key features:
* Parameterized pipelines for multiple tables
* Incremental loading using watermark column(LastModifiedDate)
* Dynamic folder and file naming
* Integration with Logic Apps for alerting

<img width="472" height="142" alt="Image" src="https://github.com/user-attachments/assets/c5dfbc61-b5bc-453b-a8ae-4d7e87c41e24" />
<img width="550" height="161" alt="Image" src="https://github.com/user-attachments/assets/a878d7eb-3c56-4912-b588-427f17063b7a" />

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

## Flow
1. Loop tables using ForEach
2. Get last value using Lookup
3. Set current time
4. Load data using Copy
5. Check data using If Condition
6. * No data → Delete file
    * Data → Update latest value
7. Send notification using Web Activity


  


