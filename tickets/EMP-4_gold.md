# EMP-4: Gold Layer - Aggregations & KPIs

**Jira Project**: Employee Data Pipeline (EMP)
**Type**: Task
**Priority**: Medium
**Status**: TODO

## Description
Implement the Gold (analytics) layer. Read from Silver and produce business-ready aggregation tables for BI consumption.

## Acceptance Criteria
- [ ] `emp_gold.dept_headcount` — employee count per department
- [ ] `emp_gold.salary_summary` — min/max/avg salary by department & job_title
- [ ] `emp_gold.attrition_monthly` — monthly terminated employee count
- [ ] `emp_gold.tenure_distribution` — years of service buckets (0-1, 1-3, 3-5, 5+)
- [ ] `emp_gold.hiring_velocity` — new hires per month/quarter
- [ ] PII masked: full_name and email excluded from Gold tables
- [ ] Feature branch `feature/EMP-4-gold` created and PR raised

## Technical Notes
- Use Spark SQL / DataFrame for aggregations
- Write as Delta tables with `overwrite` mode
- Add `_gold_refreshed_at` column to all Gold tables
