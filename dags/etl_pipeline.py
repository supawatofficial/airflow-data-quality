from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

import os
from datetime import datetime, timedelta
import logging
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import pytz


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 9, 20, tzinfo=pytz.timezone("Asia/Bangkok")),
    "schedule_interval": "@daily",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="etl_pipeline",
    default_args=default_args,
    catchup=False,
):
    def get_connection():
        """
        Establishes a SQLAlchemy engine and connection to the MySQL database.

        Returns:
            sqlalchemy.engine.Connection: An active database connection object.

        Raises:
            Exception: If the database connection fails.
        """
        logging.info("Establishing database connection...")
        try:
            host = os.getenv("MYSQL_HOST")
            port = int(os.getenv("MYSQL_PORT"))
            user = os.getenv("MYSQL_USER")
            password = os.getenv("MYSQL_PASSWORD")
            db = os.getenv("MYSQL_DB")

            engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}")
            conn = engine.connect()
            logging.info("-> Database connection established.")
            return conn
        except Exception as e:
            logging.error(f"-> Database connection failed: {str(e)}")
            raise

    def get_data(conn):
        """
        Extract all records from the 'transactions' table.

        Args:
            conn (sqlalchemy.engine.Connection): The active SQLAlchemy database connection.

        Returns:
            pandas.DataFrame: A DataFrame containing the transaction data.

        Raises:
            Exception: If the data extraction fails.
        """
        logging.info("Extracting data from 'transactions' table...")
        try:
            query = "SELECT * FROM transactions;"
            result = conn.execute(text(query)).fetchall()
            df = pd.DataFrame(result)
            logging.info(f"-> Successfully extracted {len(df)} records.")
            return df
        except Exception as e:
            logging.error(f"-> Data extraction failed: {str(e)}")
            raise

    def standardize_data(df):
        """
        Standardize data formats for consistency.

        Args:
            df (pandas.DataFrame): The input DataFrame with raw data.

        Returns:
            pandas.DataFrame: The DataFrame with standardized formats.
        """
        logging.info("Standardizing data formats...")

        def standardize_date_format(date_str):
            date_formats = ["%Y-%m-%d", "%Y/%m/%d", "%B %d, %Y"]
            for format in date_formats:
                try:
                    return pd.to_datetime(date_str, format=format)
                except (ValueError, TypeError):
                    continue
            return pd.NaT
        
        df["transaction_date"] = df["transaction_date"].apply(standardize_date_format)
        logging.info("-> Standardized 'transaction_date' column.")

        df["status"] = df["status"].str.title()
        logging.info("-> Standardized 'status' column to title case.")

        df[["street", "subdistrict", "district", "province"]] = df["shipping_address"].str.split(", ", expand=True)
        df["street"] = df["street"].str.extract(r"([A-Za-z\s\-]+) Rd\.", expand=False)
        df["province"] = df["province"].replace(r"( \d{5})$", "", regex=True)
        df["zip_code"] = df["shipping_address"].str.extract(r"(\d{5})$", expand=True)
        df.drop(columns=["shipping_address"], inplace=True)
        logging.info("-> Split 'shipping_address' into 'street', 'subdistrict', 'district', 'province', and 'zip_code' columns.")
        return df

    def correct_data_types(df):
        """
        Correct the data types of specific columns in the DataFrame.

        Args:
            df (pandas.DataFrame): The input DataFrame.

        Returns:
            pandas.DataFrame: The DataFrame with corrected data types.
        """
        logging.info("Correcting data types...")
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").astype("Int64")
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        df["discount"] = pd.to_numeric(df["discount"], errors="coerce")
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
        df["status"] = df["status"].astype(str)
        df["street"] = df["street"].astype(str)
        df["subdistrict"] = df["subdistrict"].astype(str)
        df["district"] = df["district"].astype(str)
        df["province"] = df["province"].astype(str)
        df["zip_code"] = pd.to_numeric(df["zip_code"], errors="coerce").astype("Int64")
        logging.info("-> Data types have been corrected.")
        return df

    def validate_data(df):
        """
        Validate numerical data integrity and filter out invalid records.

        Args:
            df (pandas.DataFrame): The input DataFrame.

        Returns:
            pandas.DataFrame: The DataFrame with invalid numerical values replaced by NaN
                              and invalid date records removed.
        """
        logging.info("Validating data...")

        df["quantity"] = df["quantity"].where((df["quantity"] > 0) & (df["quantity"] < 100))
        df["price"] = np.where(df["price"] > 0, df["price"], np.NaN)
        df["discount"] = np.where((df["discount"] < df["price"]) & df["discount"] >= 0, df["discount"], np.NaN)
        logging.info("-> Validated numerical columns ('quantity', 'price', 'discount').")

        df_before = len(df)
        df = df[(df["transaction_date"] >= datetime(2025, 7, 1))& (df["transaction_date"] <= datetime(2025, 7, 17))]
        df_after = len(df)
        logging.info(f"-> Filtered out {df_before - df_after} records with out-of-range dates.")
        return df

    def handle_missing_values(df):
        """
        Impute missing values for 'price' and 'quantity' columns.

        Args:
            df (pandas.DataFrame): The input DataFrame with missing values.

        Returns:
            pandas.DataFrame: The DataFrame with missing values handled.
        """
        logging.info("Handling missing values...")

        missing_price_before = df["price"].isna().sum()

        price_category_median = df.groupby("product_category")["price"].median()
        df["price"] = df["price"].fillna(df["product_category"].map(price_category_median))

        missing_price_after = df["price"].isna().sum()
        logging.info(f"-> Imputed {missing_price_before - missing_price_after} missing values for 'price' column.")

        missing_quantity_before = df["quantity"].isna().sum()

        df["quantity"] = df["quantity"].fillna(np.ceil((df["amount"] + df["discount"]) / df["price"])).astype("Int64")

        missing_quantity_after = df["quantity"].isna().sum()
        logging.info(f"-> Imputed {missing_quantity_before - missing_quantity_after} missing values for 'quantity' column.")
        return df

    def correct_computed_data(df):
        """
        Recalculate 'amount' and 'price' to ensure data consistency.

        Args:
            df (pandas.DataFrame): The input DataFrame.

        Returns:
            pandas.DataFrame: The DataFrame with corrected computed columns.
        """
        logging.info("Correcting computed columns...")

        df["amount"] = df["quantity"] * df["price"] - df["discount"]
        logging.info("-> Recalculated 'amount' column for consistency.")
        df["price"] = (df["amount"] + df["discount"]) / df["quantity"]
        logging.info("-> Recalculated 'price' column for consistency.")
        return df

    def reorder_columns(df):
        """
        Reorder and select the final set of columns for the DataFrame.

        Args:
            df (pandas.DataFrame): The input DataFrame with all processed columns.

        Returns:
            pandas.DataFrame: A DataFrame with columns in the desired final order.
        """
        logging.info("Reordering columns to final format...")
        final_columns = [
            "transaction_id",
            "customer_id",
            "transaction_date",
            "product_id",
            "product_category",
            "quantity",
            "price",
            "discount",
            "amount",
            "payment_method",
            "street",
            "subdistrict",
            "district",
            "province",
            "zip_code",
            "status",
        ]
        logging.info("-> Columns reordered successfully.")
        return df[final_columns]

    def run_data_quality_check(df):
        """
        Run data quality checks on the cleaned DataFrame.

        Args:
            df (pandas.DataFrame): The DataFrame to be checked.

        Returns:
            bool: True if all data quality checks pass, False otherwise.
        """
        logging.info("Running data quality checks...")

        quantity_null_check = df["quantity"].notnull().all()
        logging.info(f"-> Quality null check for 'quantity' not null: {'PASSED' if quantity_null_check else 'FAILED'}")

        price_null_check = df["price"].notnull().all()
        logging.info(f"-> Quality null check for 'price' not null: {'PASSED' if price_null_check else 'FAILED'}")

        amount_null_check = df["amount"].notnull().all()
        logging.info(f"-> Quality null check for 'amount' not null: {'PASSED' if amount_null_check else 'FAILED'}")

        date_null_check = df["transaction_date"].notnull().all()
        logging.info(f"-> Quality null check for 'transaction_date' not null: {'PASSED' if date_null_check else 'FAILED'}")

        street_null_check = df["street"].notnull().all()
        logging.info(f"-> Quality null check for 'street' not null: {'PASSED' if street_null_check else 'FAILED'}")

        subdistrict_null_check = df["subdistrict"].notnull().all()
        logging.info(f"-> Quality null check for 'subdistrict' not null: {'PASSED' if subdistrict_null_check else 'FAILED'}")

        district_null_check = df["district"].notnull().all()
        logging.info(f"-> Quality null check for 'district' not null: {'PASSED' if district_null_check else 'FAILED'}")

        province_null_check = df["province"].notnull().all()
        logging.info(f"-> Quality null check for 'province' not null: {'PASSED' if province_null_check else 'FAILED'}")

        zip_code_null_check = df["zip_code"].notnull().all()
        logging.info(f"-> Quality null check for 'zip_code' not null: {'PASSED' if zip_code_null_check else 'FAILED'}")

        date_type_check = pd.api.types.is_datetime64_any_dtype(df["transaction_date"])
        logging.info(f"-> Quality type check for 'transaction_date' type: {'PASSED' if date_type_check else 'FAILED'}")

        date_value_check = ((df["transaction_date"] >= datetime(2025, 7, 1)) & (df["transaction_date"] <= datetime(2025, 7, 17))).all()
        logging.info(f"-> Quality value check for 'transaction_date' between 2025-07-01 and 2025-07-17: {'PASSED' if date_value_check else 'FAILED'}")

        quantity_range_check = ((df["quantity"] >= 0) & (df["quantity"] <= 100)).all()
        logging.info(f"-> Quality range check for 'quantity' between 0 and 100: {'PASSED' if quantity_range_check else 'FAILED'}")

        price_range_check = (df["price"] > 0).all()
        logging.info(f"-> Quality range check for 'price' greater than 0: {'PASSED' if price_range_check else 'FAILED'}")

        discount_range_check = ((df["discount"] >= 0) & (df["discount"] <= df["price"])).all()
        logging.info(f"-> Quality range check for 'discount' between 0 and 'price': {'PASSED' if discount_range_check else 'FAILED'}")

        price_value_check = (((df["amount"] + df["discount"]) / df["quantity"] - df["price"]) == 0).all()
        logging.info(f"-> Quality value check for 'price' = ('amount' + 'discount') / 'quantity': {'PASSED' if price_value_check else 'FAILED'}")

        amount_value_check = ((df["amount"] - (df["quantity"] * df["price"] - df["discount"])) == 0).all()
        logging.info(f"-> Quality value check for 'amount' = 'quantity' * 'price' - 'discount': {'PASSED' if amount_value_check else 'FAILED'}")
        return all(
            [
                quantity_null_check,
                price_null_check,
                amount_null_check,
                date_null_check,
                street_null_check,
                subdistrict_null_check,
                district_null_check,
                province_null_check,
                zip_code_null_check,
                date_type_check,
                date_value_check,
                quantity_range_check,
                price_range_check,
                discount_range_check,
                price_value_check,
                amount_value_check,
            ]
        )

    def extract_raw_data():
        """Task to extract raw data from MySQL."""
        conn = get_connection()
        df = get_data(conn)
        df.to_parquet("/opt/airflow/data/transactions_raw.parquet", index=False)
        return "Data extraction completed."

    def transform_data():
        """Task to extract, transform, and load data."""
        df = pd.read_parquet("/opt/airflow/data/transactions_raw.parquet")

        logging.info("Starting data transformation steps...")
        df = standardize_data(df)
        df = correct_data_types(df)
        df = validate_data(df)
        df = handle_missing_values(df)
        df = correct_computed_data(df)
        df = reorder_columns(df)

        df.to_parquet("/opt/airflow/data/transactions_cleaned.parquet", index=False)
        return "Extraction and transformation completed."

    def validate_data_quality():
        """Task to validate the quality of the cleaned data."""
        df = pd.read_parquet("/opt/airflow/data/transactions_cleaned.parquet")

        logging.info("Starting data quality validation task...")
        quality_check_passed = run_data_quality_check(df)

        if not quality_check_passed:
            raise ValueError("Data quality check failed")
        return "Data validation completed."

    start_task = EmptyOperator(task_id="start_task")
    end_task = EmptyOperator(task_id="end_task")

    extract_raw_data_task = PythonOperator(
        task_id="extract_raw_data",
        python_callable=extract_raw_data
    )

    transform_data_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data
    )

    validate_data_quality_task = PythonOperator(
        task_id="validate_data_quality",
        python_callable=validate_data_quality
    )

    upload_local_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id="upload_local_to_gcs",
        src="/opt/airflow/data/transactions_cleaned.parquet",
        dst="airflow_data_quality/transactions_cleaned.parquet",
        bucket="supawat-workshop-bucket",
        gcp_conn_id="google_cloud_default"
    )

    upload_gcs_to_bigquery_task = GCSToBigQueryOperator(
        task_id="upload_gcs_to_bigquery",
        gcp_conn_id="google_cloud_default",
        bucket="supawat-workshop-bucket",
        source_objects=["airflow_data_quality/transactions_cleaned.parquet"],
        source_format="PARQUET",
        destination_project_dataset_table="supawat_workshop_dataset.transactions_cleaned",
        schema_fields=[
            {"name": "transaction_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "customer_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "transaction_date", "type": "DATETIME", "mode": "NULLABLE"},
            {"name": "product_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "product_category", "type": "STRING", "mode": "NULLABLE"},
            {"name": "quantity", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "price", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "discount", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "amount", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "payment_method", "type": "STRING", "mode": "NULLABLE"},
            {"name": "street", "type": "STRING", "mode": "NULLABLE"},
            {"name": "subdistrict", "type": "STRING", "mode": "NULLABLE"},
            {"name": "district", "type": "STRING", "mode": "NULLABLE"},
            {"name": "province", "type": "STRING", "mode": "NULLABLE"},
            {"name": "zip_code", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "status", "type": "STRING", "mode": "NULLABLE"},        
        ],
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        autodetect=False
    )

    start_task >> extract_raw_data_task >> transform_data_task >> validate_data_quality_task >> upload_local_to_gcs_task >> upload_gcs_to_bigquery_task >> end_task
