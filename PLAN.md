# HOSE Project 2-Week Execution Plan

**Timeline:** March 23, 2026 -> April 5, 2026  
**Goal:** Complete and demonstrate an end-to-end VN30 data pipeline (Bronze -> Silver -> Gold) with ML next-day price prediction, Airflow orchestration, Streamlit analytics, and Power BI reporting.

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
- [x] Add idempotency checks (safe reruns, no duplicate raw records).
- [x] Add backfill capability for historical dates.
- [x] Validate partition naming and storage structure.
- **Deliverable:** Rerunnable Bronze process with deterministic outputs.

### Day 5 - March 27, 2026: Silver Transformation v1
- [x] Build PySpark cleaning pipeline.
- [x] Normalize schema, handle missing values, cast data types.
- [x] Compute moving averages (`MA10`, `MA20`, `MA50`, `MA200`) for analytics readiness.
- **Deliverable:** Processed Silver Parquet written to `processed` container.

### Day 6 - March 28, 2026: Silver Data Quality
- [x] Add data quality checks (nulls, duplicates, invalid numeric values).
- [x] Generate quality metrics and logs per run.
- [x] Fail pipeline on critical quality violations.
- **Deliverable:** Validated Silver layer with quality reporting.

### Day 7 - March 29, 2026: Airflow Orchestration
- [x] Create DAG flow: Bronze -> Silver -> Gold -> ML inference.
- [x] Configure retries, scheduling, and dependency rules.
- [x] Test manual trigger and one scheduled execution.
- **Deliverable:** End-to-end DAG orchestration works with ML inference task placeholder.

## Week 2

### Day 8 - March 30, 2026: Gold Modeling
- [x] Define Star Schema for analytics (fact + dimension tables).
- [x] Create SQL DDL scripts.
- [x] Load modeled tables into Azure SQL.
- [x] Create ML-ready feature dataset view/table from Silver/Gold.
- **Deliverable:** Gold schema and ML feature dataset are available in Azure SQL.

### Day 9 - March 31, 2026: ML Training v1 (Time Series Transformer)
- [ ] Build supervised dataset from `dbo.vw_ml_features` with target `target_next_close` (T+1 close).
- [ ] Use time-based split (80/20), no shuffle.
- [ ] Implement baseline `y_pred = close_t` for benchmark.
- [ ] Train Time Series Transformer regression model (fixed random seed for reproducibility).
- [ ] Save local artifacts: `models/transformer_t1_latest.*`, scaler, and feature config.
- [ ] Log metrics: MSE (primary), RMSE/MAE (secondary), directional accuracy.
- **Deliverable:** Reproducible Transformer training pipeline with baseline-vs-model metrics.

### Day 10 - April 1, 2026: ML Evaluation + Inference
- [ ] Add Airflow tasks: `ml_train_daily`, `ml_inference_daily`, `ml_evaluate_daily`.
- [ ] Run daily retraining and next-day inference.
- [ ] Write predictions to `fact_price_predictions`.
- [ ] Write run metrics to `ml_metrics_history`.
- [ ] Target acceptance: directional accuracy > 70% with stable/declining MSE vs baseline.
- **Deliverable:** Daily ML retrain/inference pipeline running from Airflow with persisted metrics.

### Day 11 - April 2, 2026: Gold Optimization + Dashboard v1
- [ ] Add primary keys, indexes, and constraints.
- [ ] Validate query performance for top dashboard queries.
- [ ] Verify incremental load behavior (no duplicate facts).
- [ ] Build dashboard pages for historical analytics and predicted vs actual prices.
- [ ] Add model KPI cards: MSE, RMSE, MAE, directional accuracy.
- [ ] Add ticker/date filters and top predicted movers view.
- **Deliverable:** Query-efficient Gold layer and functional dashboard with ML KPI tracking.

### Day 12 - April 3, 2026: Dashboard Polish + Reliability
- [ ] Improve UX (layout clarity, chart labels, loading behavior).
- [ ] Add caching and optimize query calls.
- [ ] Add interpretation text for key visuals.
- [ ] Run end-to-end tests from clean state.
- [ ] Run rerun/backfill scenarios to prove idempotency.
- [ ] Run ML retrain/reinference soak test and document observed issues.
- [ ] Add feature-importance style explainability section (model/attention-based ranking).
- **Deliverable:** Demo-ready dashboard and reliability evidence for both data and ML pipeline.

### Day 13 - April 4, 2026: Documentation + Presentation
- [ ] Update `README.md` with architecture, setup, runbook, and ML section.
- [ ] Add production deployment section for Azure VM (Docker Compose + Nginx + HTTPS).
- [ ] Add Power BI integration section (Azure SQL source + refresh strategy + read-only account).
- [ ] Capture screenshots/log outputs for proof.
- [ ] Prepare presentation slides and demo script.
- **Deliverable:** Submission-ready documentation and presentation assets with ML evaluation results.

### Day 14 - April 5, 2026: Final Buffer + Submission
- [ ] Fix remaining bugs and rerun full pipeline once.
- [ ] Validate Azure VM deployment readiness checklist (security group, service restart, secrets, logs).
- [ ] Validate Power BI dashboard refresh against latest daily prediction run.
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
- [ ] `SC-05`: ML model improves over naive baseline on MSE and maintains directional accuracy > 70%.
- [ ] `SC-06`: Prediction table `fact_price_predictions` has zero duplicate (`ticker`, `prediction_date`) keys.
- [ ] `SC-07`: Dashboard includes "Actual vs Predicted" and model metric views.
- [ ] `SC-08`: Power BI dashboard refreshes successfully from Azure SQL after daily DAG run.
- [ ] `SC-09`: Azure VM deployment is reproducible with Docker Compose and secure ingress policy.
- [ ] `SC-10`: End-to-end run time stays within the project SLA target.

## Evidence Checklist (for grading/demo)
- [ ] Airflow DAG run screenshots/logs
- [ ] Blob `raw` and `processed` storage evidence
- [ ] Azure SQL Star Schema + sample query results
- [ ] ML training/evaluation report (baseline vs model metrics)
- [ ] Prediction table sample output from Azure SQL
- [ ] Streamlit dashboard screenshots/video (including prediction screens)
- [ ] Power BI dashboard screenshots (actual vs predicted + metric history)
- [ ] Azure VM deployment evidence (Nginx endpoint, container status, security notes)
- [ ] Updated README and final architecture diagram including ML flow
