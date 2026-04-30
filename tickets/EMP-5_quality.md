# EMP-5: Data Quality Framework

**Jira Project**: Employee Data Pipeline (EMP)
**Type**: Task
**Priority**: Medium
**Status**: TODO

## Description
Implement a reusable data quality framework that logs quality metrics (completeness, validity, uniqueness) for each pipeline run to a Delta audit table.

## Acceptance Criteria
- [ ] `emp_quality.dq_audit_log` table created with columns: `run_id`, `layer`, `table_name`, `check_name`, `total_rows`, `passed_rows`, `failed_rows`, `pass_rate`, `run_timestamp`
- [ ] DQ checks run automatically after each Bronze and Silver write
- [ ] Alert threshold: fail pipeline if pass_rate < 95% on critical checks
- [ ] Reusable Python class `DataQualityChecker` implemented
- [ ] Unit tests for DQ checker in `tests/test_transformations.py`
- [ ] Feature branch `feature/EMP-5-quality` created and PR raised

## Checks Implemented
| Check | Layer | Threshold |
|---|---|---|
| employee_id not null | Bronze/Silver | 100% |
| salary > 0 | Silver | 99% |
| No duplicate employee_id | Silver | 100% |
| valid status (ACTIVE/TERMINATED) | Silver | 99% |
| hire_date not in future | Silver | 100% |
