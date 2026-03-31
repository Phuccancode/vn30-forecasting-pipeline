# VN30 Project 2-Week Execution Plan

**Timeline:** March 23, 2026 -> April 5, 2026  
**Goal:** Complete and demonstrate an end-to-end VN30 data pipeline (Bronze -> Silver -> Gold) with ML next-day price prediction, Airflow orchestration, and a Streamlit analytics dashboard.

## Day-by-Day Plan

## Week 1

### Day 1 - March 23, 2026: Scope + Local Setup
- [x] Finalize assignment scope and measurable success criteria (including ML prediction).
- [x] Confirm local environment: Docker, Airflow, `.env` wiring.
- [x] Draft architecture diagram (Bronze/Silver/Gold + ML + orchestration + serving).
- **Deliverable:** Local Airflow starts successfully and scope checklist includes ML success gates.

### Day 2 - March 24, 2026: Azure Infrastructure
- [x] Run `infrastructure/setup_azure.sh`.
- [x] Verify Azure resources: Resource Group, Storage Account, Blob containers, SQL database.
- [x] Store required connection strings in `.env`.
- **Deliverable:** Cloud infrastructure is provisioned and validated.

### Day 3 - March 25, 2026: Bronze Ingestion v1
- [x] Implement data extraction from `vnstock`.
- [x] Save raw data as Parquet, partitioned by date.
- [x] Add basic logging and retry support.
- **Deliverable:** First successful raw load to Blob Storage.

### Day 4 - March 26, 2026: Bronze Hardening
- [ ] Add idempotency checks (safe reruns, no duplicate raw records).
- [ ] Add backfill capability for historical dates.
- [ ] Validate partition naming and storage structure.
- **Deliverable:** Rerunnable Bronze process with deterministic outputs.

### Day 5 - March 27, 2026: Silver Transformation v1
- [ ] Build PySpark cleaning pipeline.
- [ ] Normalize schema, handle missing values, cast data types.
- [ ] Compute moving averages for analytics readiness.
- **Deliverable:** Processed Silver Parquet written to `processed` container.

### Day 6 - March 28, 2026: Silver Data Quality
- [ ] Add data quality checks (nulls, duplicates, invalid numeric values).
- [ ] Generate quality metrics and logs per run.
- [ ] Fail pipeline on critical quality violations.
- **Deliverable:** Validated Silver layer with quality reporting.

### Day 7 - March 29, 2026: Airflow Orchestration
- [ ] Create DAG flow: Bronze -> Silver -> Gold -> ML inference.
- [ ] Configure retries, scheduling, and dependency rules.
- [ ] Test manual trigger and one scheduled execution.
- **Deliverable:** End-to-end DAG orchestration works with ML inference task placeholder.

## Week 2

### Day 8 - March 30, 2026: Gold Modeling
- [ ] Define Star Schema for analytics (fact + dimension tables).
- [ ] Create SQL DDL scripts.
- [ ] Load modeled tables into Azure SQL.
- [ ] Create ML-ready feature dataset view/table from Silver/Gold.
- **Deliverable:** Gold schema and ML feature dataset are available in Azure SQL.

### Day 9 - March 31, 2026: ML Training v1
- [ ] Implement baseline model (naive previous-close) for benchmark.
- [ ] Train first ML model for next-day close prediction.
- [ ] Save model artifact and training metadata.
- **Deliverable:** Reproducible ML training pipeline with baseline and candidate model metrics.

### Day 10 - April 1, 2026: ML Evaluation + Inference
- [ ] Evaluate model on validation/test split (MAE/RMSE/MAPE).
- [ ] Confirm model beats baseline by target threshold.
- [ ] Build inference job writing to `fact_price_predictions`.
- **Deliverable:** Validated model and daily prediction table populated.

### Day 11 - April 2, 2026: Gold Optimization + Dashboard v1
- [ ] Add primary keys, indexes, and constraints.
- [ ] Validate query performance for top dashboard queries.
- [ ] Verify incremental load behavior (no duplicate facts).
- [ ] Build dashboard pages for both historical analytics and predicted vs actual prices.
- [ ] Add model metric cards (MAE/RMSE/MAPE) and ticker/date filters.
- **Deliverable:** Query-efficient Gold layer and functional dashboard with ML results.

### Day 12 - April 3, 2026: Dashboard Polish + Reliability
- [ ] Improve UX (layout clarity, chart labels, loading behavior).
- [ ] Add caching and optimize query calls.
- [ ] Add interpretation text for key visuals.
- [ ] Run end-to-end tests from clean state.
- [ ] Run rerun/backfill scenarios to prove idempotency.
- [ ] Run ML retrain/reinference test and document observed issues.
- **Deliverable:** Demo-ready dashboard and reliability evidence for both data and ML pipeline.

### Day 13 - April 4, 2026: Documentation + Presentation
- [ ] Update `README.md` with architecture, setup, runbook, and ML section.
- [ ] Capture screenshots/log outputs for proof.
- [ ] Prepare presentation slides and demo script.
- **Deliverable:** Submission-ready documentation and presentation assets with ML evaluation results.

### Day 14 - April 5, 2026: Final Buffer + Submission
- [ ] Fix remaining bugs and rerun full pipeline once.
- [ ] Validate all deliverables are complete and consistent.
- [ ] Final packaging and submission.
- **Deliverable:** Final assignment submitted.

## Daily Working Pattern (Recommended)
- 60% implementation
- 20% testing and validation
- 20% documentation and evidence capture

## Success Criteria (Measurable)
- [ ] `SC-01`: End-to-end DAG executes Bronze -> Silver -> Gold -> ML inference successfully.
- [ ] `SC-02`: Ingestion coverage includes all VN30 tickers from config.
- [ ] `SC-03`: Gold `fact_prices` has zero duplicate (`ticker`, `date`) keys.
- [ ] `SC-04`: ML training is reproducible and uses a fixed random seed.
- [ ] `SC-05`: ML model improves at least 10% over naive baseline on MAE or RMSE.
- [ ] `SC-06`: Prediction table `fact_price_predictions` has zero duplicate (`ticker`, `prediction_date`) keys.
- [ ] `SC-07`: Dashboard includes "Actual vs Predicted" and model metric views.
- [ ] `SC-08`: End-to-end run time stays within the project SLA target.

## Evidence Checklist (for grading/demo)
- [ ] Airflow DAG run screenshots/logs
- [ ] Blob `raw` and `processed` storage evidence
- [ ] Azure SQL Star Schema + sample query results
- [ ] ML training/evaluation report (baseline vs model metrics)
- [ ] Prediction table sample output from Azure SQL
- [ ] Streamlit dashboard screenshots/video (including prediction screens)
- [ ] Updated README and final architecture diagram including ML flow
