# Food Inspections — Second Implementation

**DAMG-7370: Designing Advanced Data Architectures for Business Intelligence**
**Northeastern University — Spring 2026**
**Authors:** Michael Dereje (dereje.m@northeastern.edu), Gaurav Dalal (dalal.g@northeastern.edu), Pratik Kanade (kanade.pr@northeastern.edu)

---

## Overview

This is the second implementation of the Food Inspections pipeline. It processes Chicago and Dallas food inspection data through a Medallion Architecture (Raw → Bronze → Silver → Gold) on Databricks, using parameterized notebooks, metadata-driven ingestion, DQX validation with quarantine tables, and SCD Type 2 on the establishment dimension via Delta MERGE. Visualization is built in Tableau.

---

## Folder Structure

```
Second Implementation/
├── Pipeline/
│   ├── Setup and Data Exploration/
│   │   ├── setup.ipynb                    # Create schemas, volumes, metadata tables
│   │   └── data_exploration.ipynb         # Data profiling and exploration
│   │
│   ├── Pipeline Notebooks/
│   │   ├── raw_to_bronze.ipynb            # Metadata-driven CSV → Parquet → Bronze Delta
│   │   ├── chicago_bronze_to_silver.ipynb # Chicago Silver with DQX validation
│   │   ├── dallas_bronze_to_silver.ipynb  # Dallas Silver with DQX validation
│   │   └── silver_to_gold.ipynb           # Dimensions, SCD2, fact table
│   │
│   ├── job.py                             # Job definition via Databricks Python SDK
│   ├── table validation.sql               # Gold layer verification queries
│   └── Final Project.dbc                  # Databricks archive export
│
├── Data Model/
│   ├── Data Model 1.DM1                   # ER Studio model file
│   └── Data Model 1.png                   # ER diagram screenshot
│
├── Datasets/
│   ├── chicago_source_sample.csv
│   ├── dallas_source_sample.csv
│   ├── chicago_silver_sample.xlsx
│   ├── dallas_silver_sample.xlsx
│   ├── chicago_upsert_sample.csv.csv      # SCD2 test data
│   └── dallas_upsert_sample.csv.csv       # SCD2 test data
│
└── Tableau Viz/
    ├── Final Project Visualtization.twbx  # Tableau workbook
    └── semantic layer.hyper               # Tableau Hyper extract
```

---

## Pipeline Steps

### 1. Setup (`setup.ipynb`)

Run once. Creates four schemas (`final_project_raw`, `_bronze`, `_silver`, `_gold`), raw and bronze volumes, pipeline metadata tables (parent + child), and DQX quarantine tables.

All notebooks are parameterized via `dbutils.widgets` — catalog and schema names are passed as parameters, not hardcoded.

### 2. Raw → Bronze (`raw_to_bronze.ipynb`)

Reads active tables from `pipeline_metadata_parent`, loads each CSV from the raw volume, writes immutable Parquet snapshots to the bronze volume, and logs execution metrics (row counts, status, timestamp) to `pipeline_metadata_child`.

### 3. Bronze → Silver (per city)

**`chicago_bronze_to_silver.ipynb`**: Installs DQX, reads latest bronze snapshot from metadata, applies validation expectations, parses pipe-delimited violations into rows (split on `|`, regex extract code/description/comments), derives inspection scores (Pass=90, Conditions=80, Fail=70, No Entry=0). Failed rows go to `chicago_quarantine`.

**`dallas_bronze_to_silver.ipynb`**: Same DQX approach. Unpivots 25 violation column groups via `stack()`, truncates ZIP+4 to 5 digits, validates score range 0–100, applies Rules 8 and 9. Failed rows go to `dallas_quarantine`.

### 4. Silver → Gold (`silver_to_gold.ipynb`)

Builds the dimensional model:

| Table | Type | Load Strategy |
|-------|------|---------------|
| `dim_inspection` | Dimension | Full overwrite |
| `dim_violation` | Dimension | Full overwrite |
| `dim_date` | Dimension | Delta MERGE (insert new dates only) |
| `dim_comment` | Bridge | Full overwrite (inspection ↔ violation with comments) |
| `dim_establishment` | Dimension | **SCD Type 2 via Delta MERGE** |
| `fact_food_inspection` | Fact | Incremental append (excludes existing records via `left_anti` join) |

**Fact grain:** One row per inspection. FKs: `inspection_key`, `establishment_key`, `date_key`. Measure: `inspection_score`.

### SCD Type 2 — `dim_establishment`

Tracks changes to `facility_type` using Delta MERGE:
- Match on `establishment_id + license_number` where `is_current = true`
- If `facility_type` changed → set `end_date = now()`, `is_current = false`
- Insert new current record with `start_date = now()`, `is_current = true`

### Job Orchestration (`job.py`)

Defined programmatically using the Databricks Python SDK:

```
raw_to_bronze → chicago_bronze_to_silver → dallas_bronze_to_silver → silver_to_gold
```

Tasks run sequentially. Registered via `WorkspaceClient().jobs.reset()`.

---

## Differences from First Implementation

| Aspect | First Implementation | This Implementation |
|--------|---------------------|---------------------|
| **Schema layout** | Single `workspace.default` | Separate schema per layer |
| **Parameterization** | Hardcoded paths | `dbutils.widgets` |
| **Pipeline metadata** | None | Parent/child metadata tables with execution logging |
| **Data quality** | PySpark `.where()` filters, rows dropped | DQX expectations + quarantine tables for failed rows |
| **SCD2 table** | `dim_violation` (tracks description changes) | `dim_establishment` (tracks facility_type changes) |
| **SCD2 method** | Manual PySpark expire + insert + union + overwrite | Delta MERGE (`whenMatchedUpdate` + separate insert) |
| **Fact grain** | One row per violation per inspection (~1.3M rows) | One row per inspection |
| **Fact loading** | Full overwrite | Incremental append |
| **Violation–inspection link** | Directly in fact table (violation_key FK) | Bridge table `dim_comment` |
| **dim_date** | Full overwrite | Delta MERGE (insert-only) |
| **Job definition** | YAML | Python SDK |
| **BI tool** | Power BI (`.pbix`) | Tableau (`.twbx`) |
| **ER model tool** | Navicat (`.nmodel`) | ER Studio (`.DM1`) |

---

## Tools & Technologies

| Tool | Purpose |
|------|---------|
| Databricks (Unity Catalog, Delta Lake) | Data processing, storage, ETL |
| Databricks DQX | Validation, profiling, quarantine |
| Delta MERGE | SCD2 + incremental loads |
| PySpark / Spark SQL | ETL transformations |
| Databricks SDK (Python) | Job orchestration |
| Tableau | BI dashboards |
| ER Studio | Dimensional model design |
| Git | Version control |
