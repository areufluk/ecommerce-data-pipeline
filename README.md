# Ecommerce Data Pipeline

This is my first data engineering project. Designed to extract order data from PostgreSQL, process all of the data using Pyspark, store all of the data in Google Cloud storage, and split data into three layers raw data, transformed data, and refined data. Use Looker Studio to visualize data by connecting to Bigquery external tables.

### Technologies Used
* PostgreSQL: data source that store fake order data
* Google Cloud Storage: store raw data, transformed data, and refined data
* Bigquery: query refined data from external tables
* Pyspark: data transformation and processing at scale
* Apache Airflow: workflow orchestration and scheduling of ETL tasks
* Docker: containerize and run the pipeline components
