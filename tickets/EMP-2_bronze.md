# EMP-2: Bronze Layer - Raw Data Ingestion

**Jira Project**: Employee Data Pipeline (EMP)
**Type**: Task
**Priority**: High
**Status**: TODO

## Description
Implement the Bronze (raw) ingestion layer. Ingest employee CSV files into Delta Lake with no transformations — preserve raw data as-is. Add ingestion metadata columns.

## Acceptance Criteria
- [ ] PySpark notebook reads CSV from `/data/raw/employees/` path
- [ ] Data written to `emp_bronze.employees` Delta table
- [ ] Metadata columns added: `_ingested_at`, `_source_file`, `_pipeline_run_id`
- [ ] Notebook is idempotent (re-run safe using MERGE or overwrite with dedup)
- [ ] Row count validation logged
- [ ] Feature branch `feature/EMP-2-bronze` created and PR raised

## Technical Notes
- Use `spark.read.option("header", True).csv(path)`
- Write mode: `overwrite` with partition by ingestion date
- Schema: infer on first load, enforce on subsequent loads
