## Project Overview

+ Shopware is a modern retail company seeking to enhance its data infrastructure by consolidating
and processing information from diverse sources. The primary aim of this project is to design and
implement a scalable, enterprise-grade data pipeline capable of handling both batch and
streaming data. The pipeline should empower business units by providing timely, reliable, and
insightful data to support critical decision-making.

##  Project Collaboration Guidelines

###  1. **Project Structure Convention**

Encourage consistency by adopting this layout across **all pipelines**:

```
pipelines/
└── <pipeline-name>/
    ├── config/        # JSON/YAML/ENV files for that pipeline
    ├── lambda-fn/     # Lambda source code (organized per logical unit)
    ├── glue-jobs/     # Glue ETL scripts (if applicable)
    ├── fargate/       # ECS task definitions, Dockerfiles, etc.
    └── src/           # Other processor logic (e.g., batch jobs, helpers)
```

**Action**: Create a `README.md` in each `<pipeline-name>/` directory describing:

* What the pipeline does
* Entry points (e.g., Lambda handler, Glue job)
* Expected data inputs/outputs (e.g., S3 paths, streams)
* Dev/test instructions

---

###  2. **Code Responsibility Guidelines**

* **Shared logic** (e.g., data validation, schema models, utilities) should live in the `shared/` folder.
* No cross-pipeline imports unless through `shared/`. This avoids tight coupling between pipelines.

---

###  3. **Dev Environment Standards**

* Use **`requirements.txt`** (or per-pipeline `requirements.txt`) for Lambda/Glue dependencies.
* For containerized tasks in `fargate/`, enforce a consistent `Dockerfile` structure and use environment variable configs via `.env` or `taskdef.json`.

---

###  4. **Branching Strategy**

* Use the format: `feature/<pipeline>-<topic>`
  E.g., `feature/crm-validation`, `bugfix/web-traffic-lambda-timeout`
* Keep branches short-lived and opened with PR templates (suggested below).

---



###  Summary
<!-- What does this PR introduce? -->

###  Testing Steps
- [ ] Deployed to dev
- [ ] Validated output in S3 / DynamoDB / other

###  Related Pipeline
- [ ] CRM Interaction
- [ ] Inventory
- [ ] POS
- [ ] Web Traffic


---


---

### 7. **Documentation Checklist (Per Pipeline)**

In each `pipelines/<name>/README.md`, include:

* Purpose of the pipeline
* Trigger source (e.g., Kinesis, SQS, EventBridge)
* Destination (S3, DynamoDB, etc.)
* List of Lambda or Glue job scripts
* Sample event structure (if streaming)
* Retry/failure handling logic

---

##  Example: `pipelines/crm-interaction/README.md`

# CRM Interaction Pipeline

## Purpose
Processes interaction logs from Kinesis. Validates, enriches, and stores them in curated S3 paths.

## Components
- `lambda-fn/validator.py`: Validates interaction schema
- `glue-jobs/kpi_aggregator.py`: Computes daily KPIs

## Input
- Source: Kinesis → Lambda (validator)

## Output
- Valid: `s3://.../valid/`
- Invalid: `s3://.../invalid/`
- KPI: Delta table in `s3://.../kpi-delta/`

## Notes
- Glue job is triggered via EventBridge on new data
- Tracking via DynamoDB
```

---

##  Summary

| Topic          | Recommendation                                        |
| -------------- | ----------------------------------------------------- |
| File structure | One folder per pipeline, consistent subfolders        |
| Code reuse     | Centralize in `shared/`                               |
| Dev guidelines | Branch naming, per-pipeline README      |
| Docs           | README for each pipeline, global `README.md` overview |

