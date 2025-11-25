# Cloud Based Ingestion and Storage Pipeline for CDC Chronic Disease Indicators  

This README provides a step by step process to reproduce the ingestion, validation, cleaning, storage, and visualization pipeline implemented for the CDC Chronic Disease Indicators dataset using Google Cloud Platform.

---

## 1. Prerequisites

Before starting:

- Access to GCP and the class folder: **FA25-BL-INFO-I535**
- A new project named: `FA25-I535-agr-chronicdisease`

Your GitHub repository should contain:

- `ingest_validate_cdi.py`  
- `bigquery.sql`  

---

## 2. Configure GCP Project

1. Log in to: https://console.cloud.google.com  
2. Select **FA25-BL-INFO-I535** → Create a new project:
3. Under **IAM**, grant yourself only:
- Storage Admin  
- BigQuery Data Editor  
- Viewer (for Looker Studio)
4. Enable required APIs:
- Cloud Storage
- BigQuery
- Cloud Resource Manager

---

## 3. Create GCS Bucket and Folder Structure

1. Open **Cloud Storage > Buckets** → Create:
- Name: `cdi-data-lake`
- Region: `us-central1`
- Standard storage class
2. Create folders:
- meta/
- raw/
- clean/
- quarantine/
This implements a multi-zone ingestion architecture.

---

## 4. Upload the Raw CDC Dataset

1. Download the CDI dataset from the CDC website.  
2. Upload the CSV to:
   gs://cdi-data-lake/raw/
Raw data must remain unchanged.

---

## 5. Run Data Cleaning & Validation Pipeline

1. Open **Cloud Shell**.  
2. Clone the GitHub repository:
3. Run the script:
   python3 ingest_validate_cdi.py

The script performs:
- Type conversion  
- Missing value checks  
- Confidence interval validation  
- Removal of inconsistent rows  
- Writes outputs to:
  - gs://cdi-data-lake/clean/cdi_clean.csv
  - gs://cdi-data-lake/quarantine/cdi_bad_rows.csv

---

## 6. Create BigQuery Dataset and Tables

1. In BigQuery, create a dataset:
   cdi_lake
2. Create **external raw table**:

- Table: `cdi_raw_external`
- Source: `gs://cdi-data-lake/raw/*.csv`
- Format: CSV  
- Schema: Auto-detect

3. Create **native clean table**:

- Table: `cdi_clean`
- Source: `gs://cdi-data-lake/clean/*.csv`
- Format: CSV  
- Schema: Auto-detect  
- Write preference: Overwrite

---

## 7. Run Validation & EDA Queries

Open BigQuery SQL Editor → Run queries from `bigquery.sql`, including:

- Row counts  
- Year coverage  
- Topic distribution  
- State-level coverage  
- Confidence interval checks  
- Trend checks for selected topics and locations  

These confirm pipeline integrity.

---

## 8. Build Visualizations in Looker Studio

1. Open: https://lookerstudio.google.com  
2. Create a report → Add data source → BigQuery table:
   cdi_lake.cdi_clean
Create the following charts:

### Chart 1 ― Topic Coverage  
- Type: Bar  
- Dimension: Topic  
- Metric: Count  

### Chart 2 ― Records by YearStart  
- Type: Time series  
- Dimension: YearStart  

### Chart 3 ― Diabetes by State  
- Type: Geo map  
- Filters: Topic = Diabetes, Year = 2022  

### Chart 4 ― Diabetes Trend in CA by Sex  
- Type: Line chart  
- Filter: LocationAbbr = CA, Topic = Diabetes  

---

## 9. Cleanup and Cost Control

- Delete temporary Cloud Shell files  
- Set GCS lifecycle rules (optional)
- Remove unused BigQuery tables  
- Ensure all resources are within class quotas
