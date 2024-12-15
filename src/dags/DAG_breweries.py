from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from etl.extract import fetch_breweries
from etl.transform import clean_data
from etl.load import create_bronze_layer, create_silver_layer, create_gold_layer
from conn.minio_conn import get_boto3_client
from conn.minio_bucket import create_bucket

# Task to fetch brewery data from external source
def fetch_breweries_task():
    """
    Extract brewery data by calling the fetch_breweries function.
    Returns the breweries data in JSON format.
    """
    breweries = fetch_breweries()  # Fetch the brewery data
    return breweries

# Task to create the MinIO bucket for data storage
def create_bucket_task():
    """
    Create the MinIO bucket where all data layers will be stored.
    """
    boto3_client = get_boto3_client('http://minio:9000', access_key='testtamura', secret_key='testtamura')
    create_bucket(boto3_client, bucket_name='datalake-case')  # Create bucket in MinIO

# Task to load the raw brewery data into the bronze layer
def bronze_layer_task(breweries):
    """
    Uploads raw brewery data to MinIO's bronze layer.
    """
    boto3_client = get_boto3_client('http://minio:9000', access_key='testtamura', secret_key='testtamura')
    create_bronze_layer(boto3_client, breweries, bucket_name='datalake-case', file_name="bronze_breweries.json")

# Task to clean the brewery data (e.g., handle missing values, format data)
def clean_data_task():
    """
    Clean brewery data by normalizing column names, filling missing values,
    and ensuring correct data formatting.
    """
    boto3_client = get_boto3_client('http://minio:9000', access_key='testtamura', secret_key='testtamura')
    clean_data(boto3_client, bucket_name='datalake-case', raw_prefix='bronze_layer/raw', cleaned_dir='bronze_layer/cleaned')

# Task to transform cleaned data to the silver layer (parquet format)
def silver_layer_task():
    """
    Transforms and stores the cleaned data in the Silver Layer (Parquet format).
    """
    boto3_client = get_boto3_client('http://minio:9000', access_key='testtamura', secret_key='testtamura')
    create_silver_layer(boto3_client, bucket_name='datalake-case', bronze_cleaned_prefix='bronze_layer/cleaned', silver_dir='silver_layer/')

# Task to create the gold layer with aggregated brewery data (by type and state)
def gold_layer_task():
    """
    Creates the Gold Layer with aggregated brewery data, storing it as both CSV and Parquet files.
    """
    boto3_client = get_boto3_client('http://minio:9000', access_key='testtamura', secret_key='testtamura')
    create_gold_layer(boto3_client, bucket_name='datalake-case', silver_dir='silver_layer/', gold_dir='golden_layer/')

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'max_retry_delay': timedelta(minutes=30),
    'backoff_factor': 2,
    'start_date': datetime(2024, 12, 11),  # Define start date for the DAG
    'catchup': False  # No backfilling

}

# Define the DAG
dag = DAG(
    'brewery_data_pipeline',  # DAG name
    default_args=default_args,
    description='An ETL pipeline for brewery data',
    schedule='@daily',  # Schedule to run daily
    catchup=False,  # Avoid backfilling
)

# Define task sequence and dependencies

fetch_task = PythonOperator(
    task_id='fetch_breweries',
    python_callable=fetch_breweries_task,  # Task to fetch the data
    dag=dag,
    retries=3,  # Retry task 3 times if it fails
    retry_delay=timedelta(minutes=5)  # Retry after 5 minutes if task fails
)

create_bucket_task = PythonOperator(
    task_id='bucket_creation',
    python_callable=create_bucket_task,  # Task to create the bucket
    dag=dag
)

bronze_layer_task = PythonOperator(
    task_id='load_bronze_layer',
    python_callable=bronze_layer_task,  # Task to load data into the bronze layer
    dag=dag,
    retries=3,
    retry_delay=timedelta(minutes=5),
    op_args=[fetch_task.output]
)

clean_data_task = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data_task,  # Task to clean the data
    dag=dag,
    retries=3,
    retry_delay=timedelta(minutes=5)
)

silver_layer_task = PythonOperator(
    task_id='create_silver_layer',
    python_callable=silver_layer_task,  # Task to create the silver layer
    dag=dag,
    retries=3,
    retry_delay=timedelta(minutes=5)
)

gold_layer_task = PythonOperator(
    task_id='create_gold_layer',
    python_callable=gold_layer_task,  # Task to create the gold layer
    dag=dag,
    retries=3,
    retry_delay=timedelta(minutes=5)
)

# Set up task dependencies in the correct order
fetch_task >> create_bucket_task >> bronze_layer_task >> clean_data_task >> silver_layer_task >> gold_layer_task
