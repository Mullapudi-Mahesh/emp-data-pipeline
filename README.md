# Employee Data Engineering Pipeline

## Overview
End-to-end data engineering pipeline for employee analytics using the **Medallion Architecture** (Bronze → Silver → Gold) built with PySpark on Databricks.

## Use Case
Process raw employee data from HR systems to produce analytics-ready datasets for workforce planning, salary analysis, attrition reporting, and department headcount.

## Architecture
```
Raw CSV/JSON (HR System)
        ↓
  [Bronze Layer]  ← Raw ingestion, no transformations
        ↓
  [Silver Layer]  ← Cleaned, validated, deduplicated
        ↓
  [Gold Layer]    ← Business aggregations, KPIs
        ↓
  BI/Reporting (Power BI / Databricks SQL)
```

## Data Schema
### Employee Source Schema
| Column | Type | Description |
|---|---|---|
| employee_id | STRING | Unique employee identifier |
| full_name | STRING | Employee full name |
| department | STRING | Department name |
| job_title | STRING | Job title |
| salary | DOUBLE | Annual salary |
| hire_date | DATE | Hiring date |
| termination_date | DATE | Termination date (null if active) |
| status | STRING | ACTIVE / TERMINATED |
| manager_id | STRING | Reporting manager employee_id |
| location | STRING | Office location |
| email | STRING | Work email |

## Tickets
| Ticket | Description | Layer |
|---|---|---|
| EMP-1 | Project structure & raw data setup | Setup |
| EMP-2 | Bronze layer - raw data ingestion | Bronze |
| EMP-3 | Silver layer - data cleaning & validation | Silver |
| EMP-4 | Gold layer - aggregations & KPIs | Gold |
| EMP-5 | Data quality framework | Quality |

## Tech Stack
- **Processing**: PySpark 3.x on Databricks
- **Storage**: Delta Lake (ACID transactions)
- **Orchestration**: Databricks Workflows
- **Version Control**: GitHub
- **Project Tracking**: Jira

## Project Structure
```
emp_usecase/
├── README.md
├── use_case.md
├── tickets/
│   ├── EMP-1_setup.md
│   ├── EMP-2_bronze.md
│   ├── EMP-3_silver.md
│   ├── EMP-4_gold.md
│   └── EMP-5_quality.md
├── data/
│   └── sample_employees.csv
├── notebooks/
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_transformation.py
│   ├── 03_gold_aggregations.py
│   └── 04_data_quality.py
└── tests/
    └── test_transformations.py
```
