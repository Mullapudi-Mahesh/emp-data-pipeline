# Databricks notebook source
# MAGIC %md
# MAGIC # EMP-3: Silver Layer — Data Cleaning & Validation
# MAGIC
# MAGIC Reads from `emp_bronze.employees`, applies quality rules, writes clean records to
# MAGIC `emp_silver.employees` and rejected records to `emp_silver.employees_rejected`.

# COMMAND ----------

import uuid
import logging
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, DateType, TimestampType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("silver_cleaning")

# COMMAND ----------

# MAGIC %md ## 1. Configuration

# COMMAND ----------

dbutils.widgets.text("bronze_run_id", "", "Bronze Pipeline Run ID (blank = all)")
dbutils.widgets.text("run_id", "", "Silver Pipeline Run ID")

BRONZE_RUN_ID   = dbutils.widgets.get("bronze_run_id") or None
RUN_ID          = dbutils.widgets.get("run_id") or str(uuid.uuid4())
SOURCE_TABLE    = "emp_bronze.employees"
TARGET_TABLE    = "emp_silver.employees"
REJECTED_TABLE  = "emp_silver.employees_rejected"
PROCESSED_AT    = datetime.utcnow()

logger.info(f"Silver cleaning started | run_id={RUN_ID} | bronze_run_id={BRONZE_RUN_ID}")

# COMMAND ----------

# MAGIC %md ## 2. Read Bronze

# COMMAND ----------

df_bronze = spark.table(SOURCE_TABLE)

if BRONZE_RUN_ID:
    df_bronze = df_bronze.filter(F.col("_pipeline_run_id") == BRONZE_RUN_ID)

bronze_count = df_bronze.count()
logger.info(f"Bronze rows loaded: {bronze_count}")

if bronze_count == 0:
    raise ValueError("No bronze records found for the given filter.")

# COMMAND ----------

# MAGIC %md ## 3. Type Casting & Standardisation

# COMMAND ----------

df_typed = (
    df_bronze
    # Numeric types
    .withColumn("salary",           F.col("salary").cast(DecimalType(15, 2)))
    # Date types
    .withColumn("hire_date",        F.to_date(F.col("hire_date"),        "yyyy-MM-dd"))
    .withColumn("termination_date", F.to_date(F.col("termination_date"), "yyyy-MM-dd"))
    # Standardise string fields
    .withColumn("department",  F.trim(F.initcap(F.col("department"))))
    .withColumn("job_title",   F.trim(F.initcap(F.col("job_title"))))
    .withColumn("location",    F.trim(F.initcap(F.col("location"))))
    .withColumn("status",      F.trim(F.upper(F.col("status"))))
    .withColumn("email",       F.trim(F.lower(F.col("email"))))
    .withColumn("full_name",   F.trim(F.col("full_name")))
    # Split full_name into first / last (best-effort)
    .withColumn("first_name",  F.trim(F.split(F.col("full_name"), " ")[0]))
    .withColumn("last_name",   F.trim(F.element_at(F.split(F.col("full_name"), " "), -1)))
    # Carry bronze metadata
    .withColumn("_bronze_run_id",   F.col("_pipeline_run_id"))
    .withColumn("_silver_run_id",   F.lit(RUN_ID))
    .withColumn("_processed_at",    F.lit(PROCESSED_AT).cast(TimestampType()))
)

# COMMAND ----------

# MAGIC %md ## 4. Build Rejection Flags

# COMMAND ----------

df_flagged = (
    df_typed
    .withColumn("_reject_reason", F.lit(None).cast("string"))
    # Mandatory fields null
    .withColumn("_reject_reason", F.when(F.col("employee_id").isNull(),
        "NULL employee_id").otherwise(F.col("_reject_reason")))
    .withColumn("_reject_reason", F.when(F.col("_reject_reason").isNull() & F.col("department").isNull(),
        "NULL department").otherwise(F.col("_reject_reason")))
    .withColumn("_reject_reason", F.when(F.col("_reject_reason").isNull() & F.col("salary").isNull(),
        "NULL or unparseable salary").otherwise(F.col("_reject_reason")))
    # Salary range
    .withColumn("_reject_reason", F.when(
        F.col("_reject_reason").isNull() & (
            (F.col("salary") <= 0) | (F.col("salary") >= 10_000_000)
        ),
        "Salary out of range (0, 10M)").otherwise(F.col("_reject_reason")))
    # hire_date unparseable
    .withColumn("_reject_reason", F.when(
        F.col("_reject_reason").isNull() & F.col("hire_date").isNull(),
        "NULL or unparseable hire_date").otherwise(F.col("_reject_reason")))
    # Invalid status
    .withColumn("_reject_reason", F.when(
        F.col("_reject_reason").isNull() & ~F.col("status").isin("ACTIVE", "TERMINATED", "ON_LEAVE"),
        "Invalid status value").otherwise(F.col("_reject_reason")))
)

df_clean    = df_flagged.filter(F.col("_reject_reason").isNull())
df_rejected = df_flagged.filter(F.col("_reject_reason").isNotNull())

# COMMAND ----------

# MAGIC %md ## 5. Deduplication
# MAGIC
# MAGIC Keep the most-recently ingested record per employee_id.

# COMMAND ----------

from pyspark.sql.window import Window

dedup_window = Window.partitionBy("employee_id").orderBy(F.col("_ingested_at").desc())

df_clean_dedup = (
    df_clean
    .withColumn("_row_num", F.row_number().over(dedup_window))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num", "_reject_reason")
)

clean_count    = df_clean_dedup.count()
rejected_count = df_rejected.count()
logger.info(f"Clean rows: {clean_count} | Rejected rows: {rejected_count}")

# COMMAND ----------

# MAGIC %md ## 6. Write Silver Tables (Idempotent)

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS emp_silver")

# Clean table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
        employee_id       STRING,
        full_name         STRING,
        first_name        STRING,
        last_name         STRING,
        department        STRING,
        job_title         STRING,
        salary            DECIMAL(15,2),
        hire_date         DATE,
        termination_date  DATE,
        status            STRING,
        manager_id        STRING,
        location          STRING,
        email             STRING,
        _ingested_at      TIMESTAMP,
        _source_file      STRING,
        _bronze_run_id    STRING,
        _silver_run_id    STRING,
        _processed_at     TIMESTAMP
    )
    USING DELTA
    PARTITIONED BY (_silver_run_id)
    COMMENT 'Silver cleaned and validated employee data'
""")

# Rejected table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {REJECTED_TABLE} (
        employee_id       STRING,
        full_name         STRING,
        department        STRING,
        job_title         STRING,
        salary            DECIMAL(15,2),
        hire_date         DATE,
        termination_date  DATE,
        status            STRING,
        manager_id        STRING,
        location          STRING,
        email             STRING,
        _ingested_at      TIMESTAMP,
        _source_file      STRING,
        _bronze_run_id    STRING,
        _silver_run_id    STRING,
        _processed_at     TIMESTAMP,
        _reject_reason    STRING
    )
    USING DELTA
    PARTITIONED BY (_silver_run_id)
    COMMENT 'Silver rejected records with rejection reason'
""")

# Idempotent: delete this run's rows before re-inserting
for tbl in [TARGET_TABLE, REJECTED_TABLE]:
    spark.sql(f"DELETE FROM {tbl} WHERE _silver_run_id = '{RUN_ID}'")

silver_cols = [
    "employee_id", "full_name", "first_name", "last_name", "department",
    "job_title", "salary", "hire_date", "termination_date", "status",
    "manager_id", "location", "email",
    "_ingested_at", "_source_file", "_bronze_run_id", "_silver_run_id", "_processed_at",
]

df_clean_dedup.select(silver_cols).write.format("delta").mode("append").saveAsTable(TARGET_TABLE)

rejected_cols = silver_cols + ["_reject_reason"]
df_rejected.select(rejected_cols).write.format("delta").mode("append").saveAsTable(REJECTED_TABLE)

logger.info(f"Wrote {clean_count} clean rows to {TARGET_TABLE}")
logger.info(f"Wrote {rejected_count} rejected rows to {REJECTED_TABLE}")

# COMMAND ----------

# MAGIC %md ## 7. Row Count Validation

# COMMAND ----------

silver_written   = spark.table(TARGET_TABLE).filter(F.col("_silver_run_id") == RUN_ID).count()
rejected_written = spark.table(REJECTED_TABLE).filter(F.col("_silver_run_id") == RUN_ID).count()

assert silver_written == clean_count, (
    f"Clean row mismatch: expected={clean_count}, written={silver_written}"
)
assert rejected_written == rejected_count, (
    f"Rejected row mismatch: expected={rejected_count}, written={rejected_written}"
)

logger.info("Row count validation passed.")

# COMMAND ----------

# MAGIC %md ## 8. Preview

# COMMAND ----------

display(spark.table(TARGET_TABLE).orderBy(F.col("_processed_at").desc()).limit(10))

# COMMAND ----------

# MAGIC %md ## 9. Rejection Summary

# COMMAND ----------

display(
    spark.table(REJECTED_TABLE)
    .filter(F.col("_silver_run_id") == RUN_ID)
    .groupBy("_reject_reason")
    .agg(F.count("*").alias("count"))
    .orderBy(F.col("count").desc())
)

logger.info(f"Silver cleaning complete | run_id={RUN_ID} | clean={clean_count} | rejected={rejected_count}")

dbutils.notebook.exit(f'{{"run_id": "{RUN_ID}", "clean_rows": {clean_count}, "rejected_rows": {rejected_count}}}')
