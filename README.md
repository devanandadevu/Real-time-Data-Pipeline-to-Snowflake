# 📄 Real-time-Data-Pipeline-to-Snowflake

This system is designed to ingest real-time data from an API, process it through several layers, store it efficiently, and make it available for business intelligence (BI) and analytics.

---

## 🧠 Project Overview

This architecture diagram illustrates a modern data pipeline for real-time ingestion, processing, storage, and visualization. Here's a breakdown of each component and the data flow.


### 🔁 Pipeline Workflow:

API → Lambda

API triggers AWS Lambda, which handles real-time ingestion or transformation.

Lambda → Kafka (on EC2)

The Lambda function pushes the data into a Kafka cluster running on EC2, which acts as a message broker for scalable streaming data.

Kafka → AWS Glue / Spark

Kafka streams are consumed by AWS Glue jobs, leveraging Apache Spark for ETL operations such as data cleaning, transformation, and enrichment.

Glue → S3

Processed data is stored in Amazon S3, serving as the raw or processed data lake.

S3 → Snowflake (via Snowpipe)

Snowpipe, Snowflake’s continuous data ingestion service, loads data from S3 into Snowflake, enabling near real-time data availability.

Snowflake → Power BI

Power BI connects to Snowflake to visualize and analyze the ingested and transformed data.

## ## 🗺️ Architecture Diagram
<img width="1620" height="1080" alt="architecture" src="https://github.com/user-attachments/assets/78938ddf-1a98-4f4d-89bc-3f425baa9acb" />
## 🚀 Technologies Used

| Technology             | Purpose                                                      |
|-------------------     |--------------------------------------------------------------|
| AWS Lambda             | Processes data,and sends to kafka                            |
| Apache Kafka(on EC2)   | Manages real-time streaming and buffering                    |
| AWA Glue/Spark         | Cleans,transforms,and loads data to Amazon S3                |
| Amazon S3              | Stores transformed data acts as Snowflake's data source      |
| Snowflake              | Stores structured data for querying and analytics            |
| Snowpipe(Snoeflake)    | Automatically loads new data from S3 into snowflake          |
| Power BI               | Connects to snowflake for reporting, dashboards,and analytics|

---
📊 Dashboard Highlights

📈 Articles Over Time – Visualize how many articles are published daily or weekly.

📰 Top News Sources – See which publishers contribute the most content.

😊 Sentiment Summary – View article sentiment as Positive, Negative, or Neutral.

🔍 Search & Filter – Filter data by date, source, or sentiment to find specific insights.
