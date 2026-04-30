# Databricks notebook source
# MAGIC %md
# MAGIC # EMP-2: Bronze Layer — Raw Employee Data Ingestion
# MAGIC
# MAGIC Reads raw CSV from source path, adds ingestion metadata, and writes to
# MAGIC `emp_bronze.employees` Delta table. Idempotent: safe to re-run.

# COMMAND ----------

import uuid
import logging
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bronze_ingestion")

# COMMAND ----------

# MAGIC %md ## 1. Configuration

# COMMAND ----------

dbutils.widgets.text("source_path", "/dbfs/FileStore/emp_usecase/raw/employees/", "Source Path")
dbutils.widgets.text("run_id", "", "Pipeline Run ID")

SOURCE_PATH   = dbutils.widgets.get("source_path")
RUN_ID        = dbutils.widgets.get("run_id") or str(uuid.uuid4())
TARGET_TABLE  = "emp_bronze.employees"
INGESTED_AT   = datetime.utcnow()

logger.info(f"Bronze ingestion started | run_id={RUN_ID} | source={SOURCE_PATH}")

# COMMAND ----------

# MAGIC %md ## 2. Define Source Schema

# COMMAND ----------

SOURCE_SCHEMA = StructType([
    StructField("employee_id",       StringType(), True),
    StructField("full_name",         StringType(), True),
    StructField("department",        StringType(), True),
    StructField("job_title",         StringType(), True),
    StructField("salary",            StringType(), True),
    StructField("hire_date",         StringType(), True),
    StructField("termination_date",  StringType(), True),
    StructField("status",            StringType(), True),
    StructField("manager_id",        StringType(), True),
    StructField("location",          StringType(), True),
    StructField("email",             StringType(), True),
])

# COMMAND ----------

# MAGIC %md ## 3. Read Raw CSV

# COMMAND ----------

df_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .schema(SOURCE_SCHEMA)
    .csv(SOURCE_PATH)
)

raw_count = df_raw.count()
logger.info(f"Raw rows read: {raw_count}")

if raw_count == 0:
    raise ValueError(f"No data found at path: {SOURCE_PATH}")

# COMMAND ----------

# MAGIC %md ## 4. Add Ingestion Metadata Columns

# COMMAND ----------

df_bronze = (
    df_raw
    .withColumn("_ingested_at",      F.lit(INGESTED_AT).cast(TimestampType()))
    .withColumn("_source_file",      F.input_file_name())
    .withColumn("_pipeline_run_id",  F.lit(RUN_ID))
)

# COMMAND ----------

# MAGIC %md ## 5. Write to Bronze Delta Table (Idempotent)
# MAGIC
# MAGIC Strategy: delete rows from this run_id first, then insert — ensures
# MAGIC exactly-once semantics on re-run without MERGE overhead.

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS emp_bronze")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
        employee_id       STRING,
        full_name         STRING,
        department        STRING,
        job_title         STRING,
        salary            STRING,
        hire_date         STRING,
        termination_date  STRING,
        status            STRING,
        manager_id        STRING,
        location          STRING,
        email             STRING,
        _ingested_at      TIMESTAMP,
        _source_file      STRING,
        _pipeline_run_id  STRING
    )
    USING DELTA
    PARTITIONED BY (_pipeline_run_id)
    COMMENT 'Bronze raw employee data — no transformations applied'
""")

# Delete any previously loaded data for this run (idempotency)
spark.sql(f"""
    DELETE FROM {TARGET_TABLE}
    WHERE _pipeline_run_id = '{RUN_ID}'
""")

# Append this run's data
df_bronze.write.format("delta").mode("append").saveAsTable(TARGET_TABLE)

logger.info(f"Wrote {raw_count} rows to {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %md ## 6. Row Count Validation

# COMMAND ----------

bronze_count = spark.table(TARGET_TABLE).filter(
    F.col("_pipeline_run_id") == RUN_ID
).count()

assert bronze_count == raw_count, (
    f"Row count mismatch: read={raw_count}, written={bronze_count}"
)

logger.info(f"Row count validation passed: {bronze_count} rows for run_id={RUN_ID}")

# COMMAND ----------

# MAGIC %md ## 7. Preview

# COMMAND ----------

display(spark.table(TARGET_TABLE).orderBy(F.col("_ingested_at").desc()).limit(10))

# COMMAND ----------

# MAGIC %md ## 8. Summary Stats

# COMMAND ----------

summary = (
    spark.table(TARGET_TABLE)
    .filter(F.col("_pipeline_run_id") == RUN_ID)
    .groupBy("department", "status")
    .agg(F.count("*").alias("row_count"))
    .orderBy("department", "status")
)

display(summary)

logger.info(f"Bronze ingestion complete | run_id={RUN_ID} | rows={bronze_count}")

# Return values for downstream notebooks
dbutils.notebook.exit(f'{{"run_id": "{RUN_ID}", "rows_ingested": {bronze_count}}}')
