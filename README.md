# 🚀 Build ELT with Apache Airflow + dbt + Snowflake  
*A complete data pipeline for automated stock analytics*

This project demonstrates a production-style **ELT workflow** using:

- 🌀 **Apache Airflow (Docker)** for orchestration  
- 📈 **YFinance** for stock market data extraction  
- ❄️ **Snowflake** as the cloud data warehouse  
- 🧱 **dbt** for transformation, modeling, tests, and snapshots  
- 💾 **GitHub** for versioning & collaboration

---

## 🔥 Architecture

```mermaid
graph LR
A[YFinance API] -->|Extract| B(Airflow ETL)
B -->|Load RAW Data| C[Snowflake RAW Layer]
C -->|Transform Models| D[dbt - Analytics Layer]
D -->|Tests + Quality| E[dbt Test]
E -->|Historical Tracking| F[dbt Snapshot]
F -->|Analytics Ready| G[BI Tools]

---


LAB2_Group_10/
│
├── dags/
│   └── Lab2_ELT_with_DBT.py        # Airflow DAG (dbt run → test → snapshot)
│
├── dbt/
│   └── LAB2_Group_10/
│       ├── models/
│       │   ├── raw_stocks_data.sql                  # Creates view from RAW layer
│       │   ├── stock_metrics/
│       │   │   ├── moving_avg.sql
│       │   │   ├── price_momentum.sql
│       │   │   ├── rsi.sql
│       │   │   └── volatility.sql
│       │   └── schema.yml                           # dbt Tests (unique, not null)
│       ├── snapshots/
│       │   └── stock_snapshot.sql                   # Tracks historical changes
│       ├── dbt_project.yml                          # dbt config
│       └── profiles.yml                             # Snowflake credentials (Airflow)
│
├── docker-compose.yaml                              # Airflow + dbt environment
└── README.md
