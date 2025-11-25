💻 End-to-End ELT & Analytics Pipeline for Stock Market Insights:

🗃️ Project Overview: 

This project implements a complete, automated data analytics ecosystem designed to ingest, process, transform, and visualize historical stock market data. The goal is to generate actionable trading indicators such as Moving Averages, RSI, Price Momentum and Volatility using reliable, scalable, and fully automated pipelines.

The system integrates industry-standard tools— Airflow, Snowflake, dbt, and Power BI to deliver a modern ELT workflow from data ingestion to decision-ready dashboards.


💡 Key Features:

Automated Data Ingestion:
Collects daily historical stock data (e.g., AAPL, NVDA) using the YFinance API.

Cloud Data Warehouse Storage:
Organizes both raw and analytics tables in Snowflake, enabling scalable processing and fast querying.

Transformation Using dbt (ELT Approach):
Derives technical indicators such as Moving Averages, RSI, Price Momentum, and Volatility directly inside the warehouse.

Workflow Orchestration with Airflow:
Schedules and manages ingestion + transformation tasks via Airflow DAG for continuous pipeline execution.

Interactive Visualization with Power BI:
Visualizes stock metrics via intuitive dashboards that support trend exploration and comparison.


🧩 Component Breakdown:

📥 1. Data Collection

Pulls last 90+ days of stock data via the YFinance API.

Captures Symbol, Date, Open, High, Low, Close, Volume.

Automated ingestion executed through Apache Airflow.

🧊 2. Centralized Data Storage

Raw and transformed tables stored in Snowflake schemas.

Provides structured, performant, analytics-friendly access.

📊 3. Data Transformation (dbt)

Implements ELT transformations inside Snowflake.

Generates meaningful indicators:

1. Moving Averages (14 & 50 Day)

2. RSI

3. Price Momentum

4. Volatility

Produces clean, analytics-ready tables for dashboarding.


🔁 4. Pipeline Automation (Airflow)

DAG controls data loading + transformation sequence.

Ensures daily updates without manual effort.

📈 5. Data Visualization (Power BI)

Converts technical metrics into interactive dashboards.

Enables trend comparison and market signal identification.


🛠️ Technology Stack:

| Tool               | Purpose                                |
| ------------------ | -------------------------------------- |
| **Apache Airflow** | Pipeline orchestration & scheduling    |
| **Snowflake**      | Scalable cloud data warehouse          |
| **dbt**            | In-warehouse transformations (ELT)     |
| **YFinance API**   | Stock market data source               |
| **Power BI**       | Dashboarding & analytics visualization |


📌 Key Metrics Generated:

| Metric                            | Purpose                                         |
| --------------------------------- | ----------------------------------------------- |
| **Moving Averages (14/50 Day)**   | Detect short & long-term price trends           |
| **Relative Strength Index (RSI)** | Identify market reversals (overbought/oversold) |
| **Price Momentum**                | Measure directional strength of price changes   |
| **Volatility**                    | Assess market risk and fluctuations             |


🎯 Project Goals:

Build a fully automated pipeline for stock analytics.

Transform raw market data into insightful decision-making metrics.

Demonstrate scalable, cloud-native ELT architecture.

Provide easy-to-use dashboards to support market analysis.


📌 Conclusion:

This project successfully demonstrates a modern, production-style ELT analytics pipeline for stock market data using Airflow, Snowflake, dbt, and Power BI. By automating ingestion, transformations, testing, and reporting, the system converts raw stock data into actionable financial indicators such as RSI, Moving Averages, Price Momentum, and Volatility.
The pipeline ensures scalability, automation, and data reliability, enabling continuous market monitoring without manual intervention. The final Power BI dashboards translate complex analytics into intuitive visual insights, supporting informed investment decisions. Overall, the project showcases how cloud data engineering + warehouse-native transformation + business intelligence can work together to deliver high-impact, data-driven market intelligence.
