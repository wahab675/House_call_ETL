This project automates the extraction, transformation, and loading (ETL) of data from Housecall Pro into Snowflake and Elasticsearch using Python.

The pipeline fetches data from the Housecall Pro API, cleans and structures it using Pandas, and then uploads it into Snowflake for analytics and Elasticsearch for fast search and visualization.

It demonstrates a full data engineering workflow, from raw API data ingestion to data warehouse and search indexing integration.

⚙️ Tech Stack:

Python (Pandas, Requests)

Snowflake (Data Warehouse)

Elasticsearch (Search & Analytics Engine)

ETL Workflow Design

API Integration

📦 Features:

Fetches real-time data from Housecall Pro APIs

Cleans and validates data using Pandas

Loads structured data into Snowflake tables

Indexes data into Elasticsearch for fast querying

Logs each stage for transparency and debugging

🚀 Workflow Overview:

Extract: Pull data from Housecall Pro API using Python

Transform: Clean, normalize, and format data

Load (to Snowflake): Store cleaned data for analytics

Load (to Elasticsearch): Enable real-time data search and visualization
