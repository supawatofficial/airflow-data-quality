# Data Quality Pipeline with Airflow, GCS, and BigQuery

## Overview

This project was inspired by a Blue on Data video about Data Quality. It demonstrates an end-to-end data quality and ELT (Extract, Load, Transform) pipeline using Apache Airflow. The pipeline extracts transactional data from a MySQL database, performs extensive cleaning and validation using Pandas, stages the cleaned data on Google Cloud Storage (GCS), and finally loads it into Google BigQuery for analysis.

## Features

-   **Data Extraction**: Connects to a MySQL database to extract raw transaction data.
-   **Data Cleaning & Transformation**:
    -   **Standardization**: Unifies various date formats (`YYYY-MM-DD`, `YYYY/MM/DD`, `Month DD, YYYY`) into a single datetime format. Normalizes text fields like `status` to Title Case.
    -   **Feature Engineering**: Parses a single `shipping_address` string into structured fields: `street`, `subdistrict`, `district`, `province`, and `zip_code`.
    -   **Type Correction**: Ensures all columns have the correct data types (e.g., numeric, datetime, string).
    -   **Validation**: Replaces invalid numerical values with `NaN` (e.g., quantities outside the range (0, 100), non-positive prices) and filters out records with out-of-range transaction dates.
    -   **Imputation**: Handles missing values by imputing `price` based on the product category's median and calculating missing `quantity` from `amount`, `price`, and `discount`.
    -   **Consistency Checks**: Recalculates `amount` and `price` to ensure integrity between related columns.
-   **Data Quality Checks**:
    -   A dedicated task runs a comprehensive suite of checks on the cleaned data.
    -   Validates for null values in critical columns.
    -   Verifies data types and value ranges.
    -   Confirms the correctness of computed financial columns (`amount`, `price`).
    -   The pipeline fails if any quality check does not pass, preventing bad data from reaching the destination.
-   **Data Loading**:
    -   Saves the cleaned data as a Parquet file.
    -   Uploads the Parquet file to a Google Cloud Storage (GCS) bucket.
    -   Loads the data from GCS into a specified Google BigQuery table, truncating the existing table for idempotency.

## Project Structure

```
.
├── dags/               # Airflow DAG definitions (data_quality.py)
├── data/               # Local data staging area (for Parquet files)
├── cred/               # GCP credentials (e.g., cred.json)
├── init-script/        # SQL script to initialize the source MySQL database
├── docker-compose.yaml # Docker Compose file for running services
├── requirements.txt    # Python dependencies
└── README.md           # This file
```

## Setup Instructions

### Prerequisites

-   Docker and Docker Compose
-   Git
-   A Google Cloud Platform (GCP) account.
-   A GCP Service Account with permissions for GCS (`Storage Admin`) and BigQuery (`BigQuery Admin`).

### Installation

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/your-username/airflow-data-quality.git
    cd airflow-data-quality
    ```

2.  **Add GCP Credentials:**
    Download the JSON key for your GCP Service Account and place it in the `cred/` directory with the filename `cred.json`.

3.  **Initialize Airflow:**
    Run the `airflow-init` service to set up the Airflow database, create necessary directories, and apply initial configurations.
    ```bash
    docker-compose up airflow-init
    ```

4.  **Start Services:**
    Start all services (Airflow webserver, scheduler, MySQL database) in detached mode.
    ```bash
    docker-compose up -d
    ```

## Usage

1.  Access the Airflow UI at `http://localhost:8080`. The default credentials are `airflow` / `airflow`.
2.  The `google_cloud_default` connection is automatically configured by the environment variables in `docker-compose.yaml`. No manual connection setup is needed.
3.  Find the `etl_pipeline` DAG in the UI, un-pause it, and trigger it manually.
4.  Monitor the pipeline's progress in the Grid or Graph view.
5.  Once the pipeline succeeds, you can verify the cleaned data in your specified BigQuery table (`supawat_workshop_dataset.transactions_cleaned`).

## Common Issues and Solutions

-   **MySQL Connection Issues:**
    -   Ensure the `mysql` service is running and healthy by checking `docker-compose ps`.
    -   Verify that the environment variables in your `.env` file match the credentials expected by the MySQL service in `docker-compose.yaml`.

-   **GCP Connection / Permission Errors:**
    -   Ensure the `cred/cred.json` file exists and is a valid service account key.
    -   In the GCP Console, verify that the service account has the required roles (e.g., `Storage Admin`, `BigQuery Admin`) on the target project, bucket, and dataset.

-   **`validate_data_quality` Task Fails:**
    -   This is an expected failure if the data cleaning logic is imperfect or if the source data has new, unhandled quality issues.
    -   Check the task logs in the Airflow UI. The logs will print exactly which data quality check failed (e.g., `Quality null check for 'quantity' not null: FAILED`).
    -   Use this information to debug and improve the transformation functions in `dags/data_quality.py`.