from airflow import DAG
from datetime import datetime, timedelta

# Custom module imports
from includes.s3_utils import save_parquet_to_s3, upload_to_s3
from includes.load_parquet_to_postgres import load_parquet_to_postgres

# Airflow provider imports
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

# Define DAG folder path for mounted Docker volume
dag_folder = "/opt/airflow/dags"
sql_file = "create_table.sql"  # Use just the filename

default_args = {
    'owner': "chisom",
    'start_date': datetime(2025, 2, 26),
    'retries': 2,
    'retry_delay': timedelta(seconds=5),
    'execution_timeout': timedelta(minutes=10),
}

with DAG(
    dag_id="travel_agency_dag",
    default_args=default_args,
    description="A simple DAG to extract data from an API, load it to S3, transform it, and load it into Redshift",
    default_view="graph",
    tags=["travel_agency", "cde"],
    schedule_interval="@daily",
    catchup=False,
    template_searchpath=f"{dag_folder}/travel_agent/includes/sql",
) as dag:

    # Task 1: Load data to S3
    load_data_to_s3 = PythonOperator(
        task_id="load_data_to_S3",
        python_callable=upload_to_s3
    )

    # Task 2: Transform data
    transform_data = PythonOperator(
        task_id = "transform_data",
        python_callable = save_parquet_to_s3
    )

    # Task 3: Create table in Redshift
    
    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="postgres_default",
        sql=sql_file,     # Path to your .sql file
    )

    # Task 4: Load transformed data into Redshift
    load_data_to_postgres = PythonOperator(
        task_id="load_data_to_postgres",
        python_callable=load_parquet_to_postgres
    )

    # Define task dependencies
    load_data_to_s3 >> transform_data >> create_table >> load_data_to_postgres
