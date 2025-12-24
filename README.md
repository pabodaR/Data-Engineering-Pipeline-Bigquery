# Data-Engineering-Pipeline-Bigquery
End-to-end Python data pipeline that orchestrates dataset generation, normalizes all monetary values to USD, validates data quality, and loads results into Google BigQuery with idempotent batch loads and structured logging.

---
## Setup 
### 1. Open Cloud Shell
- Go to the GCP Console: https://console.cloud.google.com/
- Click the Cloud Shell icon
- Wait for the shell to initialize.

### 2. Clone GitHub Repository
- git clone <your-repo-url>
- cd <your-repo-name>


### 3. Install Dependencies
- pip3 install --user pandas google-cloud-bigquery
  
- pandas is for CSV manipulation.

- google-cloud-bigquery is for interacting with BigQuery.

### 4. Set Your Active GCP Project
- gcloud config set project <YOUR_PROJECT_ID>
  
- Replace <YOUR_PROJECT_ID> with your actual project ID.

### 5. Create BiqQuery Dataset
- bq mk <YOUR_DATASET_NAME>
  
- Replace <YOUR_DATASET_NAME> with your actual dataset name.

### 6. Update Pipeline Configuration
- nano pipe.py
- Open pipe.py in the cloud shell editor

- PROJECT_ID = "<YOUR_PROJECT_ID>"
- DATASET = "<YOUR_DATASET_NAME>"

- Replace <YOUR_PROJECT_ID>,<YOUR_DATASET_NAME> with your actual project id and dataset name.
---

### 7. Run Pipeline

- python3 pipeline.py

---
## What the Pipeline Does

- Orchestrates execution of all dataset scripts

- Validates generated output CSVs before loading

- Detects and converts all monetary fields to USD

- Applies BigQuery SQL schemas (DDL)

- Loads data using idempotent upserts (no duplicates)

- Emits a summary report per run

---
## Data Quality & Validation

Before loading, the pipeline validates:

- Schema and required fields

- Currency normalization to USD

- Row counts, rejected records, null value checks

- USD min/max sanity checks

- Failures stop execution with a non-zero exit code.
---
## BigQuery & Storage

- Batch loading is used for all BigQuery ingestion

Trade-offs:

- Batch loading is efficient and cost-effective for bulk datasets

- Streaming inserts provide near-real-time ingestion but are slower per row and incur higher costs
---
## Observability

- Structured JSON logs for all pipeline stages

- Clear error messages on validation or load failures

- Summary metrics emitted after execution
---
## Idempotency

- Safe to run multiple times

- ensure updates instead of duplicate inserts

