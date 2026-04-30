# EMP-1: Project Structure & Raw Data Setup

**Jira Project**: Employee Data Pipeline (EMP)
**Type**: Task
**Priority**: High
**Status**: TODO

## Description
Set up the foundational project structure, create sample raw employee data, and configure Delta Lake database schemas for all three medallion layers.

## Acceptance Criteria
- [ ] Sample CSV file with 100+ employee records created in `data/`
- [ ] Delta Lake database `emp_bronze`, `emp_silver`, `emp_gold` created in Databricks
- [ ] Project folder structure matches README
- [ ] Git repo initialized, `main` branch pushed to GitHub
- [ ] Feature branch `feature/EMP-1-setup` created and PR raised

## Tasks
1. Create sample employee CSV dataset
2. Create Databricks notebook for schema/database initialization
3. Initialize git repo and push to GitHub
