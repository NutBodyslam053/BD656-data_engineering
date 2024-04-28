import os
import json
import pandas as pd
import pytz
from datetime import datetime

from google.cloud import storage

from airflow.decorators import dag, task
from airflow.models import Variable, DagRun
from airflow.exceptions import AirflowFailException, AirflowSkipException


# Date and time
LOCAL_TIMEZONE = pytz.timezone("Asia/Bangkok")

# Airflow DAG
DAG_ID = "reddit_dataengineering"
DATA_SOURCE = "api"

# Reddit
REDDIT_CLIENT_ID = Variable.get("reddit_client_id")
REDDIT_SECRET_KEY = Variable.get("reddit_secret_key")
REDDIT_USERNAME = Variable.get("reddit_username")
REDDIT_PASSWORD = Variable.get("reddit_password")

# GCP
PROJECT_ID = "envilink"
GCP_CONNECTION_ID = "gcp"
SERVICE_ACCOUNT_KEY_PATH = Variable.get("service_account_key_path")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_KEY_PATH

# GCS
BUCKET_NAME_RAW = "envilink_raw"
BUCKET_NAME_PROCESSED = "envilink_processed"
DATA_ORGANIZATION = "reddit"
DATASET_NAME = "dataengineering"
FOLDER_PATH_PROCESSED = f"{DATA_ORGANIZATION}/{DATASET_NAME}"
PARTITION_COLS = ["province_en", "ingest_date"]

# BiqQuery
DESTINATION_TABLE = f"{DATA_ORGANIZATION}.{DATASET_NAME}"
# PROVINCE_EN = ["Phuket"]
SCHEMA_FIELDS = Variable.get(
    "schema_fields_reddit_dataengineering", deserialize_json=True
)

# Pandas
DISTRICTS_FILEPATH = Variable.get("districts_filepath")
PROVINCES_FILEPATH = Variable.get("provinces_filepath")
GEOGRAPHY_FILEPATH = Variable.get("geography_filepath")


@dag(
    dag_id=DAG_ID,
    # schedule="59 7 * * *",
    schedule="@once",
    start_date=datetime(2024, 4, 2, 0, tzinfo=LOCAL_TIMEZONE),
    catchup=False,
    tags=[DATA_SOURCE.upper()],
)
def reddit_dataengineering():
    @task
    # def create_datetime_constant(*, ti=None, dag_run: DagRun = None) -> None:
    def create_datetime_constant(ti=None, **context) -> None:
        EXECUTION_TIME = context["execute_date"].astimezone(LOCAL_TIMEZONE)
        CURRENT_DATE = EXECUTION_TIME.strftime("%Y%m%d")  # e.g. "20240302"
        CURRENT_TIME = EXECUTION_TIME.strftime("%H%M")  # e.g. "0759"
        INGEST_DATE = EXECUTION_TIME.strftime("%Y-%m-%d")  # e.g. "2024-03-02"
        INGEST_DATETIME = EXECUTION_TIME.strftime(
            "%Y-%m-%d %H:%M:%S"
        )  # e.g. "2024-03-02 08:00:00"
        FOLDER_PATH_RAW = f"{DATA_ORGANIZATION}/{DATASET_NAME}/{CURRENT_DATE}"
        FILE_NAME_RAW = f"{CURRENT_DATE}_{CURRENT_TIME}_{DATASET_NAME}_raw.json"
        ti.xcom_push(key="EXECUTION_TIME", value=EXECUTION_TIME)
        ti.xcom_push(key="INGEST_DATE", value=INGEST_DATE)
        ti.xcom_push(key="INGEST_DATETIME", value=INGEST_DATETIME)
        ti.xcom_push(key="FOLDER_PATH_RAW", value=FOLDER_PATH_RAW)
        ti.xcom_push(key="FILE_NAME_RAW", value=FILE_NAME_RAW)

    @task
    def api_to_gcs_raw(ti=None) -> None:
        import requests
        import requests.auth

        FOLDER_PATH_RAW = ti.xcom_pull(
            task_ids="create_datetime_constant", key="FOLDER_PATH_RAW"
        )
        FILE_NAME_RAW = ti.xcom_pull(
            task_ids="create_datetime_constant", key="FILE_NAME_RAW"
        )

        # Fetch data from reddit API
        if Variable.get("reddit_access_token"):
            ACCESS_TOKEN = Variable.get("reddit_access_token")
        else:
            client_auth = requests.auth.HTTPBasicAuth(
                REDDIT_CLIENT_ID, REDDIT_SECRET_KEY
            )
            post_data = {
                "grant_type": "password",
                "username": REDDIT_USERNAME,
                "password": REDDIT_PASSWORD,
            }
            headers = {"User-Agent": REDDIT_USERNAME}

            response = requests.post(
                "https://www.reddit.com/api/v1/access_token",
                auth=client_auth,
                data=post_data,
                headers=headers,
            )

            ACCESS_TOKEN = response.json()["access_token"]

        url = r"https://oauth.reddit.com/r/dataengineering/new"
        headers = {
            "Authorization": f"bearer {ACCESS_TOKEN}",
            "User-Agent": REDDIT_USERNAME,
        }

        response = requests.get(
            url=url,
            headers=headers,
            params={"limit": 100},
        )

        response.raise_for_status()

        try:
            json_data = response.json()

        except requests.exceptions.JSONDecodeError:
            raise AirflowFailException(
                f"""
                The status code was 200, but failed to decode response from {url} to JSON format.
                Response text:
                {response.text}
                """
            )

        # Upload response in JSON format to GCS
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME_RAW)
        file_path = f"{FOLDER_PATH_RAW}/{FILE_NAME_RAW}"
        blob = bucket.blob(file_path)
        blob.upload_from_string(
            data=json.dumps(json_data), content_type="application/json"
        )
        print(f"Uploaded '{file_path}' to bucket '{BUCKET_NAME_RAW}' successfully.")

    @task
    def gcs_raw_to_gcs_processed(ti=None) -> None:
        from utils.reddit_dataengineering_clean import (
            reddit_dataengineering_clean,
        )

        FOLDER_PATH_RAW = ti.xcom_pull(
            task_ids="create_datetime_constant", key="FOLDER_PATH_RAW"
        )
        FILE_NAME_RAW = ti.xcom_pull(
            task_ids="create_datetime_constant", key="FILE_NAME_RAW"
        )
        EXECUTION_TIME = ti.xcom_pull(
            task_ids="create_datetime_constant", key="EXECUTION_TIME"
        )

        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME_RAW)
        file_path = f"{FOLDER_PATH_RAW}/{FILE_NAME_RAW}"
        blob = bucket.blob(file_path)
        file_content = blob.download_as_string().decode("utf-8")

        json_data = json.loads(file_content)

        df_transformed = reddit_dataengineering_clean(
            json_data=json_data,
            schema_fields=SCHEMA_FIELDS,
            ingest_datetime=EXECUTION_TIME,
        )

        print(df_transformed.info(), end=f"\n{'-'*100}\n")

        df_transformed.to_parquet(
            path=f"gs://{BUCKET_NAME_PROCESSED}/{FOLDER_PATH_PROCESSED}",
            index=False,
            partition_cols=PARTITION_COLS,
            engine="pyarrow",
        )
        print(
            f"Uploaded '{FILE_NAME_RAW}' to bucket '{BUCKET_NAME_PROCESSED}' successfully."
        )

    @task
    def gcs_processed_to_bq_pcd(ti=None) -> None:
        INGEST_DATE = ti.xcom_pull(
            task_ids="create_datetime_constant", key="INGEST_DATE"
        )
        INGEST_DATETIME = ti.xcom_pull(
            task_ids="create_datetime_constant", key="INGEST_DATETIME"
        )

        sel = [
            ("ingest_date", "==", INGEST_DATE),
        ]
        df = pd.read_parquet(
            path=f"gs://{BUCKET_NAME_PROCESSED}/{FOLDER_PATH_PROCESSED}",
            filters=sel,
        )
        df_provinces = df[df["ingest_datetime"] == INGEST_DATETIME].copy()

        if len(df_provinces):
            df_provinces.to_gbq(
                destination_table=DESTINATION_TABLE,
                project_id=PROJECT_ID,
                if_exists="append",
                table_schema=SCHEMA_FIELDS,
            )
        else:
            raise AirflowSkipException(
                f"""
                Filtered data returned nothing.
                filters: {sel}
                ingest_datetime: {INGEST_DATETIME}
                avaliable ingest_datetime: {df["ingest_datetime"].unique()}
                """
            )

    task_start_datetime = create_datetime_constant()

    extract_pcd = api_to_gcs_raw()
    transform_pcd = gcs_raw_to_gcs_processed()
    load_pcd = gcs_processed_to_bq_pcd()

    task_start_datetime >> extract_pcd >> transform_pcd >> load_pcd


reddit_dataengineering()
