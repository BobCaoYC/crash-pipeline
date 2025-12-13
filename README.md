# Chicago Crash ML Pipeline (Extractor → MinIO → Silver → DuckDB Gold → Streamlit)

A containerized, end-to-end data engineering + ML system that pulls Chicago traffic crash data from public APIs, processes it through a multi-stage pipeline, and serves a trained machine learning model through an interactive Streamlit application. The stack also includes full observability with Prometheus metrics and Grafana dashboards.

---

## 1) Objectives

### The problem it solves
Chicago crash records are spread across multiple related datasets (crashes, vehicles, and people). On their own, these tables are hard to use for analysis or machine learning because they require consistent keys, joining logic, and careful cleaning. This project automates the process of collecting the data, building a clean analytics-ready dataset, and training a model that predicts crash outcomes.

### APIs / datasets used
This pipeline pulls data from the Chicago Data Portal (Socrata Open Data API). It retrieves crash-level records and related vehicle/person tables, then consolidates them into a single usable dataset.

### How data moves through the system
1. **Extractor (Go)** downloads raw data from the API and stores it in **MinIO** (object storage).
2. **Transformer (Python)** reads raw files and produces cleaned/merged **Silver** CSV outputs.
3. **Cleaner (Python)** applies stricter cleaning rules and writes a **Gold** dataset into **DuckDB** for fast querying.
4. **Streamlit** loads the Gold tables, trains an ML model, and supports interactive predictions and visual analytics.
5. **Monitoring** runs continuously: each service exposes metrics which **Prometheus** scrapes and **Grafana** visualizes.

### What the ML model predicts
The model predicts **[REPLACE WITH YOUR TARGET]**, for example:
- whether a crash is **high severity**
- whether a crash results in **injury/fatality**
- or another defined label derived from the Gold dataset

The prediction is intended for decision support (dashboards, triage, analysis), not automated enforcement.

### What the Streamlit app can do
The Streamlit UI provides:
- dataset exploration (filters, summary stats, EDA)
- model training and evaluation (metrics, thresholding, comparisons)
- predictions on new inputs or selected records
- visualizations and reports powered by the Gold DuckDB tables

---

## 2) Pipeline components (walkthrough)

### Extractor (Go)
The extractor pulls crash, vehicle, and person data from the Socrata API and writes the results into MinIO as raw objects. It is designed to support repeatable ingestion (streaming or backfill) and exposes Prometheus metrics for request counts, failures, and latency. This stage is responsible for reliable data acquisition and raw-layer persistence.

### Transformer (Python)
The transformer reads raw objects from MinIO, performs joins/merges across crash-vehicle-person tables, and applies lightweight cleaning and normalization. It outputs “Silver” CSVs that are consistent, typed, and ready for downstream processing. This stage focuses on building an integrated analytical dataset without enforcing final “Gold” quality constraints.

### Cleaner (Python)
The cleaner applies stricter cleaning rules, feature engineering, and quality checks (e.g., missing value handling, type enforcement, deduplication policies). It writes the finalized “Gold” dataset into DuckDB, creating tables that are optimized for fast queries from Streamlit and notebooks. This is the authoritative layer used for modeling and reporting.

### Streamlit app
The Streamlit app provides an interactive interface to the Gold DuckDB database. It supports training an ML model on the engineered features, evaluating results, and generating predictions. It also exposes visuals (EDA, metrics, and reports) to make the pipeline outputs usable for non-technical stakeholders.

### Docker Compose
Docker Compose orchestrates the entire environment: MinIO for storage, the pipeline services (extractor/transformer/cleaner), Streamlit for UI, and Prometheus + Grafana for monitoring. This ensures a reproducible setup where the whole pipeline can be started with a single command.

### Monitoring (metrics + dashboards)
Each service exposes a `/metrics` endpoint (Prometheus format). Prometheus scrapes these metrics, and Grafana dashboards visualize pipeline health: durations, row counts, error rates, and last-run timestamps. Monitoring is treated as a first-class part of the system to support debugging and operational reliability.

---

## 3) Screenshots / video proof (required)

If you do not already have screenshots, record them again. Place files under `docs/screenshots/` and update the links below.

### Extractor running
![Extractor running](docs/screenshots/extractor-running.png)

### Transformer running
![Transformer running](docs/screenshots/transformer-running.png)

### Cleaner running
![Cleaner running](docs/screenshots/cleaner-running.png)

### Streamlit app
**Home**
![Streamlit home](docs/screenshots/streamlit-home.png)

**Train model**
![Streamlit train](docs/screenshots/streamlit-train.png)

**Prediction page**
![Streamlit predict](docs/screenshots/streamlit-predict.png)

### DuckDB tables (CLI or notebook)
![DuckDB tables](docs/screenshots/duckdb-tables.png)

### Grafana dashboard (metrics)
![Grafana dashboard](docs/screenshots/grafana-dashboard.png)

### Prometheus targets (services up)
![Prometheus targets](docs/screenshots/prometheus-targets.png)

Optional: add a short screen recording link here:
- Demo video: [ADD LINK]

---


