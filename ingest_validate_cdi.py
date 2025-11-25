import json
import os
from datetime import datetime

import pandas as pd
from google.cloud import storage

# --- CONFIG ---
PROJECT_ID = "FA25-I535-agr-chronicdisease-Lake"
BUCKET_NAME = "fa25-i535-agr-chronicdisease-cdi-lake"

RAW_PREFIX = "raw"
CLEAN_PREFIX = "clean/cdi"
QUAR_PREFIX = "quarantine/cdi"
META_SCHEMA_PATH = "meta/schema/cdi_schema.json"
META_VALIDATION_PREFIX = "meta/validation"

RAW_BLOB_NAME = None  


def get_gcs_client():
    return storage.Client(project=PROJECT_ID)


def download_from_gcs(bucket, blob_name, local_path):
    client = get_gcs_client()
    b = client.bucket(bucket)
    blob = b.blob(blob_name)
    blob.download_to_filename(local_path)
    print(f"Downloaded gs://{bucket}/{blob_name} -> {local_path}")


def upload_to_gcs(bucket, local_path, blob_name):
    client = get_gcs_client()
    b = client.bucket(bucket)
    blob = b.blob(blob_name)
    blob.upload_from_filename(local_path)
    print(f"Uploaded {local_path} -> gs://{bucket}/{blob_name}")


def load_schema(bucket, schema_blob):
    client = get_gcs_client()
    b = client.bucket(bucket)
    blob = b.blob(schema_blob)
    schema_str = blob.download_as_text()
    return json.loads(schema_str)


def pick_latest_raw_blob(bucket):
    """If RAW_BLOB_NAME is not set, pick the latest file in raw/ by name."""
    client = get_gcs_client()
    b = client.bucket(bucket)
    blobs = list(b.list_blobs(prefix=f"{RAW_PREFIX}/"))
    # ignore "folders"
    blobs = [bl for bl in blobs if not bl.name.endswith("/")]
    if not blobs:
        raise RuntimeError("No raw files found in bucket.")
    latest = sorted(blobs, key=lambda x: x.name)[-1]
    return latest.name


def main():
    run_ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

    # Decides which raw blob to use
    raw_blob = RAW_BLOB_NAME or pick_latest_raw_blob(BUCKET_NAME)
    print(f"Using raw blob: {raw_blob}")

    # 1) Downloads raw CSV from GCS to local
    local_raw = "cdi_raw.csv"
    download_from_gcs(BUCKET_NAME, raw_blob, local_raw)

    # 2) Loads schema
    schema = load_schema(BUCKET_NAME, META_SCHEMA_PATH)
    required_fields = [f["name"] for f in schema["fields"] if f.get("required")]

    # 3) Reads CSV
    df = pd.read_csv(local_raw)

    # 4) Basic column presence check
    missing_cols = [c for c in required_fields if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    # 5) Light string cleaning: strips spaces and normalizes LocationAbbr
    str_cols = df.select_dtypes(include=["object", "string"]).columns
    for col in str_cols:
        df[col] = df[col].astype(str).str.strip()

    if "LocationAbbr" in df.columns:
        df["LocationAbbr"] = df["LocationAbbr"].str.upper()

    # 6) Type + null validation
    errors = []

    # Coerces numeric fields based on schema
    numeric_fields = [f["name"] for f in schema["fields"] if f["type"] in ("integer", "float")]
    for col in numeric_fields:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Required non null check
    for col in required_fields:
        null_mask = df[col].isna()
        if null_mask.any():
            for idx in df[null_mask].index:
                errors.append((idx, col, "required_field_null"))

    # Year sanity check (between 1990 and 2050)
    for col in ["YearStart", "YearEnd"]:
        if col in df.columns:
            bad_year_mask = (df[col] < 1990) | (df[col] > 2050)
            bad_year_mask = bad_year_mask.fillna(False)
            for idx in df[bad_year_mask].index:
                errors.append((idx, col, "year_out_of_range"))

    # Percent range check for DataValue when unit is %
    if {"DataValue", "DataValueUnit"}.issubset(df.columns):
        mask_percent = df["DataValueUnit"] == "%"
        # invalid if <=0 or >100 or NaN
        bad_percent_mask = mask_percent & (
            (df["DataValue"] < 0) | (df["DataValue"] > 100) | df["DataValue"].isna()
        )
        bad_percent_mask = bad_percent_mask.fillna(False)
        for idx in df[bad_percent_mask].index:
            errors.append((idx, "DataValue", "percent_out_of_range_or_null"))

    # 7) Marks invalid rows
    df["__row_valid__"] = True
    df["__error_reason__"] = ""

    for idx, col, reason in errors:
        if idx < len(df):  # safety check
            df.at[idx, "__row_valid__"] = False
            existing = df.at[idx, "__error_reason__"]
            df.at[idx, "__error_reason__"] = (existing + ";" + reason).strip(";")

    # 8) drops exact duplicates on key fields
    key_cols = [c for c in ["YearStart", "LocationAbbr", "Topic", "Question", "StratificationCategory1", "Stratification1"]
                if c in df.columns]
    if key_cols:
        before_dups = len(df)
        df = df.drop_duplicates(subset=key_cols + ["DataValue"])
        after_dups = len(df)
        print(f"Removed {before_dups - after_dups} exact duplicate rows based on {key_cols} + DataValue.")

    # Recomputes validity masks after deduplication
    df_clean = df[df["__row_valid__"]].drop(columns=["__row_valid__", "__error_reason__"])
    df_quar = df[~df["__row_valid__"]].copy()

    # 9) Partitions clean by YearStart
    os.makedirs("out_clean", exist_ok=True)
    os.makedirs("out_quarantine", exist_ok=True)

    if "YearStart" not in df_clean.columns:
        raise ValueError("YearStart column missing in clean data; cannot partition.")

    years = sorted(df_clean["YearStart"].dropna().unique().tolist())
    for y in years:
        part = df_clean[df_clean["YearStart"] == y]
        out_path = f"out_clean/cdi_clean_year_{int(y)}.csv"
        part.to_csv(out_path, index=False)
        upload_to_gcs(
            BUCKET_NAME,
            out_path,
            f"{CLEAN_PREFIX}/year={int(y)}/cdi_clean_year_{int(y)}.csv"
        )

    # Writes to Quarantine output
    if not df_quar.empty:
        quar_path = f"out_quarantine/cdi_quarantine_{run_ts}.csv"
        df_quar.to_csv(quar_path, index=False)
        upload_to_gcs(
            BUCKET_NAME,
            quar_path,
            f"{QUAR_PREFIX}/cdi_quarantine_{run_ts}.csv"
        )
    else:
        print("No invalid rows; no quarantine file created.")

    # 10) Writes validation summary
    summary = {
        "run_ts": run_ts,
        "raw_blob": raw_blob,
        "total_rows": int(len(df)),
        "valid_rows": int(len(df_clean)),
        "invalid_rows": int(len(df_quar)),
        "required_fields": required_fields,
    }
    summary_path = f"validation_summary_{run_ts}.json"
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)
    upload_to_gcs(
        BUCKET_NAME,
        summary_path,
        f"{META_VALIDATION_PREFIX}/validation_{run_ts}.json"
    )

    print("Validation summary:", summary)


if __name__ == "__main__":
    main()