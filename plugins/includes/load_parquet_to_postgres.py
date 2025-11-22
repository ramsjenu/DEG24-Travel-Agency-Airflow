from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
from io import BytesIO

def load_parquet_to_postgres(**context):
    
    bucket = "vrams-travel-agency-bucket"
    key = "processed_data/processed_data.parquet"

    # Load AWS connection from Airflow
    s3_hook = S3Hook(aws_conn_id="aws_default")
    
    # Download file from S3
    obj = s3_hook.get_key(key, bucket_name=bucket)
    file_bytes = obj.get()["Body"].read()

    df = pd.read_parquet(BytesIO(file_bytes))

    # ---- Connect to Postgres as before ----
    import psycopg2
    conn = psycopg2.connect(
        host="postgres-mstr",
        dbname="vramsdb",
        user="admin",
        password="root"
    )
    cur = conn.cursor()

    for _, row in df.iterrows():
        cur.execute(
            """
            INSERT INTO public.countries_data
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )
            ON CONFLICT (country_code) DO NOTHING
            """,
            tuple(row.values)
        )

    conn.commit()
    cur.close()
    conn.close()
