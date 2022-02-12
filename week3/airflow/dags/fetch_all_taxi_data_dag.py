import os
import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')


def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))
    print(f'created parquet file from {src_file}')


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    print('start upload')
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)
    print('upload successful')


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="fetch_all_taxi_dag",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2020, 1, 3),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de'],
) as dag:
    for taxi_type in ["yellow", "green", "fhv"]:
        dataset_file = f'{taxi_type}_tripdata_{{{{ execution_date.strftime(\'%Y-%m\') }}}}.csv'
        dataset_url = f"https://s3.amazonaws.com/nyc-tlc/trip+data/{dataset_file}"
        parquet_file = dataset_file.replace('.csv', '.parquet')
        download_dataset_task = BashOperator(
            task_id=f"download_dataset_task_{taxi_type}",
            bash_command=f"curl -sSLf {dataset_url} > {path_to_local_home}/{dataset_file}"
        )

        format_to_parquet_task = PythonOperator(
            task_id=f"format_to_parquet_task_{taxi_type}",
            python_callable=format_to_parquet,
            op_kwargs={
                "src_file": f"{path_to_local_home}/{dataset_file}",
            },
        )

        local_to_gcs_task = PythonOperator(
            task_id=f"local_to_gcs_task_{taxi_type}",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": f"raw/{parquet_file}",
                "local_file": f"{path_to_local_home}/{parquet_file}",
            },
        )

        bigquery_external_table_task = BigQueryCreateExternalTableOperator(
            task_id=f"bigquery_external_table_task_{taxi_type}",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": f"external_table_{taxi_type}_taxis",
                },
                "externalDataConfiguration": {
                    "sourceFormat": "PARQUET",
                    "sourceUris": [f"gs://{BUCKET}/raw/{parquet_file}"],
                },
            },
        )

        cleanup_task = BashOperator(
            task_id=f"delete_data_{taxi_type}",
            bash_command=f"rm {path_to_local_home}/{dataset_file} {path_to_local_home}/{parquet_file}"
        )

        download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> bigquery_external_table_task >> cleanup_task

