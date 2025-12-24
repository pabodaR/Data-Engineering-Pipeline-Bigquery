import subprocess
import sys
import json
import pandas as pd
from google.cloud import bigquery

PROJECT_ID = "YOUR_PROJECT_ID"
DATASET = "YOUR_DATASET_NAME"
client = bigquery.Client(project=PROJECT_ID)

# Define Exchange Rates
exchange_rates = {
    "USD": 1.0, "CAD": 0.75, "EUR": 1.1, "GBP": 1.25, "MXN": 0.055,
    "CNY": 0.14, "INR": 0.012, "JPY": 0.0073, "SGD": 0.74, "BRL": 0.19,
    "ARS": 0.005, "CLP": 0.0011, "ZAR": 0.054, "NGN": 0.0023, "EGP": 0.032,
    "AUD": 0.66, "NZD": 0.62
}

# Structured Logging
def log(level, message, **kwargs):
    payload = {
        "level": level,
        "message": message,
        **kwargs
    }
    print(json.dumps(payload))

# Fucntion Defnitions

# Convert Currency Values to USD
def convert_to_usd_inplace(input_csv, output_csv, columns_to_convert, currency_col="Currency"):
    """Convert specified columns to USD in-place"""
    try:
        df = pd.read_csv(input_csv) # read CSV file
        for col in columns_to_convert:
            df[col] = df.apply(lambda x: x[col] * exchange_rates[x[currency_col]], axis=1) #currency conversion
        df.to_csv(output_csv, index=False) #write to CSV
        return output_csv  #return output csv path
    except Exception as e:
        log("ERROR", "USD conversion failed", file=input_csv, error=str(e)) #log failure during conversion
        sys.exit(1)

#Count rows in the CSV
def csv_row_count(path):
    """Count rows excluding header"""
    try:
        return sum(1 for _ in open(path)) - 1
    except Exception as e:
        log("ERROR", "Failed to count CSV rows", file=path, error=str(e)) #log failure during counting
        sys.exit(1)

#Data Validation before loading
def validate_csv_before_load(csv_file, expected_cols, usd_cols=None):
    """Pre-load CSV validation"""
    try:
        df = pd.read_csv(csv_file) #read csv

        # Column count check
        if len(df.columns) != expected_cols:
            log(
                "ERROR",
                "Column count mismatch",
                file=csv_file,
                expected=expected_cols,
                found=len(df.columns)
            )
            sys.exit(1)

        # Null value check
        null_counts = df.isnull().sum().to_dict()
        bad_nulls = {k: v for k, v in null_counts.items() if v > 0}
        if bad_nulls:
            log(
                "ERROR",
                "Null values detected before load",
                file=csv_file,
                nulls=bad_nulls
            )
            sys.exit(1)

        # USD numeric sanity
        if usd_cols:
            for col in usd_cols:
                if not pd.api.types.is_numeric_dtype(df[col]):
                    log("ERROR", "Non-numeric USD column", file=csv_file, column=col)
                    sys.exit(1)
                if (df[col] < 0).any():
                    log("ERROR", "Negative USD values detected", file=csv_file, column=col)
                    sys.exit(1)

        log("INFO", "Pre-load validation passed", file=csv_file)
    except Exception as e:
        log("ERROR", "Pre-load validation failed", file=csv_file, error=str(e))
        sys.exit(1)

# Orchestrate dataset scripts

scripts = [
    "attendance_dataset_3m.py",
    "financial_data.py",
    "sales_dataset_3m.py"
]

for script in scripts:
    log("INFO", "Running dataset script", script=script)
    result = subprocess.run(["python3", script])
    if result.returncode != 0:
        log("ERROR", "Dataset script failed", script=script)
        sys.exit(1)

log("INFO", "All dataset scripts executed")

# Apply schemas

schemas = ["schemas/attendance.sql", "schemas/sales.sql", "schemas/financial.sql"]
for schema in schemas:
    try:
        with open(schema) as f:
            client.query(f.read()).result()
        log("INFO", "Schema applied", schema=schema)
    except Exception as e:
        log("ERROR", "Schema application failed", schema=schema, error=str(e))
        sys.exit(1)

# USD Conversion
# Sales CSV
sales_usd_csv = convert_to_usd_inplace(
    "sales_dataset_3m.csv",
    "sales_usd.csv",
    ["UnitPrice", "TotalSales"]
)
#Financials CSV
financial_usd_csv = convert_to_usd_inplace(
    "financial_dataset_3m.csv",
    "financial_usd.csv",
    ["Revenue", "Expense", "Profit"]
)

# Data Load + Data Quality
loads = [
    ("attendance_dataset_3m.csv", "attendance", 9, []),
    (sales_usd_csv, "sales", 9, ["UnitPrice", "TotalSales"]),
    (financial_usd_csv, "financials", 9, ["Revenue", "Expense", "Profit"])
]

for csv_file, table, expected_cols, usd_cols in loads:
    table_id = f"{PROJECT_ID}.{DATASET}.{table}"

    # Pre-load validation
    validate_csv_before_load(csv_file, expected_cols, usd_cols)

    csv_rows = csv_row_count(csv_file)
    log("INFO", "Starting load", table=table, csv_rows=csv_rows)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        field_delimiter=",",
        quote_character='"',
        allow_quoted_newlines=True,
        autodetect=False,
        write_disposition="WRITE_TRUNCATE" #Idempotency Load
    )

    try:
        with open(csv_file, "rb") as f:
            load_job = client.load_table_from_file(f, table_id, job_config=job_config)
            load_job.result()
    except Exception as e:
        log("ERROR", "BigQuery load failed", table=table, error=str(e)) # Log error during loading
        sys.exit(1)

    rejected_rows = len(load_job.errors) if load_job.errors else 0
    if rejected_rows > 0:
        log("ERROR", "Rejected rows detected", table=table, errors=load_job.errors)
        sys.exit(1)

    # BigQuery Row count validation against CSV row count 
    row_count_query = f"SELECT COUNT(*) AS cnt FROM `{table_id}`"
    bq_rows = list(client.query(row_count_query))[0].cnt
    if bq_rows != csv_rows:
        log(
            "ERROR",
            "Row count mismatch",
            table=table,
            csv_rows=csv_rows,
            loaded_rows=bq_rows
        )
        sys.exit(1)

    # USD sanity checks
    usd_stats = {}
    for col in usd_cols:
        q = f"SELECT MIN({col}) AS min_val, MAX({col}) AS max_val FROM `{table_id}`"
        r = list(client.query(q))[0]
        if r.min_val is None or r.max_val is None or r.min_val < 0:
            log(
                "ERROR",
                "USD sanity check failed",
                table=table,
                column=col,
                min=r.min_val,
                max=r.max_val
            )
            sys.exit(1)
        usd_stats[col] = {"min": r.min_val, "max": r.max_val} # Log minimum and Maximum values in currency value columns

    # Emit Data Quality Report
    log(
        "INFO",
        "Data quality report",
        table=table, #table name
        csv_rows=csv_rows, #CSV row count
        loaded_rows=bq_rows, #BigQuery row count
        rejected_rows=rejected_rows, #rejected row count
        usd_sanity=usd_stats #usd sanity check
    )

log("INFO", "pipeline_completed")
