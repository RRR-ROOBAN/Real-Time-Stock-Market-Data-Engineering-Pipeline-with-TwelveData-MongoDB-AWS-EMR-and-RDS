📈 Stock Price Data Pipeline Project
Problem Statement
Build a scalable, automated data pipeline that fetches stock price data every 5 minutes for top companies like Apple, Google, Tesla, Microsoft, and Nvidia using the TwelveData API. The system should:

Store raw data in MongoDB,

Transform it into CSV format using PySpark on AWS EMR,

Aggregate it using AWS Glue, and

Store the final, processed data in AWS RDS (MySQL/PostgreSQL) for analytics and alerting.

Business Use Cases
Real-time stock trend monitoring for financial analysts

Historical stock data archiving for research and reporting

Automated price alerts for trading bots

Scalable infrastructure for financial data aggregation

Solution Approach
1. Ingestion
Airflow DAG scheduled every 5 minutes.

Pull stock data from TwelveData API.

2. Validation
Apply schema validation and anomaly detection on the ingested data.

3. Storage
Store raw JSON data into MongoDB.

Implement time-based partitioning for efficient querying.

4. Processing
Use PySpark running on AWS EMR to:

Process raw JSON data

Convert into CSV format

Save processed files into Amazon S3 with partitioning.

5. Aggregation
AWS Glue jobs to:

Aggregate stock data into 30-minute and 1-hour summaries.

6. Analytics Layer
Load the aggregated data into AWS RDS (MySQL/PostgreSQL).

Setup RDS triggers to monitor and create real-time alerts based on stock price thresholds.

7. Automation
CI/CD Pipelines to automate:

Airflow DAG deployment

Spark script deployment

Glue job deployments

Continuous monitoring of pipeline health through Airflow and AWS CloudWatch.
