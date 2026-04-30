# Databricks notebook source
# MAGIC %md
# MAGIC # EMP-1: Schema Initialization
# MAGIC Creates Delta Lake databases for Bronze, Silver, Gold, and Quality layers.

# COMMAND ----------

import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("schema_init")

RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")
logger.info(f"Schema init started | run_id={RUN_ID}")

# COMMAND ----------

# MAGIC %md ## 1. Create Databases

# COMMAND ----------

databases = ["emp_bronze", "emp_silver", "emp_gold", "emp_quality"]

for db in databases:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    logger.info(f"Database ensured: {db}")

# COMMAND ----------

# MAGIC %md ## 2. Define Bronze Schema (DDL)

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS emp_bronze.employees (
    employee_id     STRING,
    full_name       STRING,
    department      STRING,
    job_title       STRING,
    salary          STRING,
    hire_date       STRING,
    termination_date STRING,
    status          STRING,
    manager_id      STRING,
    location        STRING,
    email           STRING,
    _ingested_at    TIMESTAMP,
    _source_file    STRING,
    _pipeline_run_id STRING
)
USING DELTA
COMMENT 'Bronze raw employee data - no transformations applied'
""")

logger.info("Table created: emp_bronze.employees")

# COMMAND ----------

# MAGIC %md ## 3. Define Silver Schema (DDL)

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS emp_silver.employees (
    employee_id          STRING      NOT NULL,
    department           STRING,
    job_title            STRING,
    salary               DOUBLE,
    hire_date            DATE,
    termination_date     DATE,
    status               STRING,
    manager_id           STRING,
    location             STRING,
    _ingested_at         TIMESTAMP,
    _silver_processed_at TIMESTAMP,
    _pipeline_run_id     STRING
)
USING DELTA
PARTITIONED BY (department)
COMMENT 'Silver cleaned employee data - validated, deduplicated, standardized'
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS emp_silver.employees_rejected (
    employee_id      STRING,
    full_name        STRING,
    department       STRING,
    job_title        STRING,
    salary           STRING,
    hire_date        STRING,
    termination_date STRING,
    status           STRING,
    manager_id       STRING,
    location         STRING,
    email            STRING,
    rejection_reason STRING,
    _ingested_at     TIMESTAMP,
    _pipeline_run_id STRING
)
USING DELTA
COMMENT 'Silver rejected records with rejection reason'
""")

logger.info("Tables created: emp_silver.employees, emp_silver.employees_rejected")

# COMMAND ----------

# MAGIC %md ## 4. Define Gold Schema (DDL)

# COMMAND ----------

gold_ddls = {
    "emp_gold.dept_headcount": """
        CREATE TABLE IF NOT EXISTS emp_gold.dept_headcount (
            department          STRING,
            active_count        BIGINT,
            terminated_count    BIGINT,
            total_count         BIGINT,
            _gold_refreshed_at  TIMESTAMP
        ) USING DELTA COMMENT 'Gold: employee headcount per department'
    """,
    "emp_gold.salary_summary": """
        CREATE TABLE IF NOT EXISTS emp_gold.salary_summary (
            department          STRING,
            job_title           STRING,
            min_salary          DOUBLE,
            max_salary          DOUBLE,
            avg_salary          DOUBLE,
            median_salary       DOUBLE,
            employee_count      BIGINT,
            _gold_refreshed_at  TIMESTAMP
        ) USING DELTA COMMENT 'Gold: salary statistics by department and job title'
    """,
    "emp_gold.attrition_monthly": """
        CREATE TABLE IF NOT EXISTS emp_gold.attrition_monthly (
            year                INT,
            month               INT,
            department          STRING,
            terminated_count    BIGINT,
            active_start_count  BIGINT,
            attrition_rate_pct  DOUBLE,
            _gold_refreshed_at  TIMESTAMP
        ) USING DELTA COMMENT 'Gold: monthly attrition rate by department'
    """,
    "emp_gold.tenure_distribution": """
        CREATE TABLE IF NOT EXISTS emp_gold.tenure_distribution (
            department          STRING,
            tenure_bucket       STRING,
            employee_count      BIGINT,
            pct_of_dept         DOUBLE,
            _gold_refreshed_at  TIMESTAMP
        ) USING DELTA COMMENT 'Gold: years-of-service distribution'
    """,
    "emp_gold.hiring_velocity": """
        CREATE TABLE IF NOT EXISTS emp_gold.hiring_velocity (
            year                INT,
            quarter             INT,
            month               INT,
            department          STRING,
            new_hires           BIGINT,
            _gold_refreshed_at  TIMESTAMP
        ) USING DELTA COMMENT 'Gold: hiring velocity by month/quarter/department'
    """,
}

for table, ddl in gold_ddls.items():
    spark.sql(ddl)
    logger.info(f"Table created: {table}")

# COMMAND ----------

# MAGIC %md ## 5. Define Quality Audit Log Schema

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS emp_quality.dq_audit_log (
    run_id          STRING,
    layer           STRING,
    table_name      STRING,
    check_name      STRING,
    total_rows      BIGINT,
    passed_rows     BIGINT,
    failed_rows     BIGINT,
    pass_rate       DOUBLE,
    threshold       DOUBLE,
    status          STRING,
    run_timestamp   TIMESTAMP
)
USING DELTA
COMMENT 'Data quality audit log for all pipeline layers'
""")

logger.info("Table created: emp_quality.dq_audit_log")

# COMMAND ----------

# MAGIC %md ## 6. Validation — List All Tables

# COMMAND ----------

for db in databases:
    tables = spark.sql(f"SHOW TABLES IN {db}").collect()
    logger.info(f"{db}: {[t.tableName for t in tables]}")

logger.info(f"Schema init complete | run_id={RUN_ID}")
