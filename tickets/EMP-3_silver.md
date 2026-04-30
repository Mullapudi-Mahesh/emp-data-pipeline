# EMP-3: Silver Layer - Data Cleaning & Validation

**Jira Project**: Employee Data Pipeline (EMP)
**Type**: Task
**Priority**: High
**Status**: TODO

## Description
Implement the Silver (cleaned) layer. Read from Bronze, apply data quality rules, standardize formats, deduplicate, and write to `emp_silver.employees`.

## Acceptance Criteria
- [ ] Read from `emp_bronze.employees`
- [ ] Null checks: `employee_id`, `department`, `salary` must not be null
- [ ] Salary must be > 0 and < 10,000,000
- [ ] `hire_date` parsed to proper DateType
- [ ] `status` standardized to UPPER CASE (ACTIVE/TERMINATED)
- [ ] Duplicates removed (keep latest by `_ingested_at`)
- [ ] Rejected records written to `emp_silver.employees_rejected` with rejection reason
- [ ] Feature branch `feature/EMP-3-silver` created and PR raised

## Technical Notes
- Use PySpark DataFrame API, not SQL (for testability)
- Add `_silver_processed_at` timestamp column
- Partition Silver table by `department`
