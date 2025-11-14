📌 Objective:

This project aims to create a fully automated customer data pipeline that generates synthetic data, ingests it into Snowflake, and maintains both current (SCD1) and historical (SCD2) records. The goal is to demonstrate real-world ETL automation using Python, NiFi, S3, and Snowflake, while implementing incremental change tracking for accurate analytics.

🧩 Problem Statement:

- Customer datasets often require frequent updates and historical tracking. Organizations need a system to:

- Load new customer data efficiently

- Maintain the latest snapshot for reporting (SCD1)

- Preserve historical changes (SCD2) for audit and analytics

- Automate the pipeline to reduce manual intervention

- This project addresses these requirements by combining synthetic data generation, cloud ingestion, and Snowflake automation.

🛠 What the Project Does:

- Generates synthetic customer data using Python Faker

- Uploads CSV files to Amazon S3 using NiFi

- Loads raw data into Snowflake staging table (customer_raw) via Snowpipe

- Performs upsert operations on the current table (customer) using a stored procedure

- Tracks changes via Snowflake streams (customer_table_changes)

- Updates the historical table (customer_history) using view logic and scheduled tasks

📀 Technologies Used:

- Python: Faker, Pandas

- Apache NiFi: Data ingestion & orchestration

- Amazon S3: Cloud storage

- Snowflake: Raw, Current, and History tables; Streams; Views; Stored Procedures; Tasks

- CSV: Data exchange format

✅ How to Run:

- Run the Python Faker script to generate CSV files.

- Use NiFi to upload files to the S3 bucket.

- Snowpipe will automatically ingest files into customer_raw.

- Stored Procedure pdr_scd_demo() performs upserts to customer.

- Stream customer_table_changes detects changes.

- View v_customer_change_data and task tsk_scd_hist update customer_history.

📌 Highlights:

- Fully automated incremental ETL pipeline

- Maintains SCD1 and SCD2 logic

- Real-time tracking using Snowflake streams and tasks
