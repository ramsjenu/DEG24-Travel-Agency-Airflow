####################################################################################
# # Using this version if not working wih airflow
#import boto3
#import os
#from boto3.session import Session  # Import Session explicitly

#from dotenv import load_dotenv
# # Load environment variables from .env file
#load_dotenv()

# def create_session():
#    session = Session(
    #    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    #    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    #    region_name=os.getenv('AWS_REGION')
#         region_name = "eu-west-1"
#    )

#    print("Ran successfully")

#    return session

# print(create_session())


####################################################################################
# CREATING A SESSION TO CONNECT TO AWS
# Import necessary libraries
import boto3
from airflow.models import Variable

def create_session():
    """Initialize and return a Boto3 session using Airflow variables."""
    aws_access_key_id = Variable.get("aws_access_key_id")
    aws_secret_access_key = Variable.get("aws_secret_access_key")
    region_name = "eu-west-1"

    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name
    )

    return session

# print(create_session())


####################################################################################
# UPLOADS RAW DATA TO S3 BUCKET
####################################################################################
# UPLOADS RAW DATA TO S3 BUCKET
import awswrangler as wr
from io import BytesIO

from includes.extract_data import  get_data

def upload_to_s3():
    """
    Uploads a pandas DataFrame to an S3 bucket with a fixed file name.
    """
    data = get_data()

    if data is None or data.empty:
        print("The DataFrame is empty. No data to upload")
        return
    
    bucket_name = "vrams-travel-agency-bucket"
    file_key = "raw_data/data.parquet"  # Specify the exact file name

    # Convert DataFrame to bytes
    buffer = BytesIO()
    data.to_parquet(buffer, index=False)
    buffer.seek(0)

    # Upload file to S3
    s3_client = create_session().client ('s3')
    s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=buffer.getvalue())

    print(f"Data successfully uploaded to s3://{bucket_name}/{file_key}")

upload_to_s3()


###############################################################################
# RETRIEVS RAW DATA, TRANSFROMS IT AND LOADS IT BACK TO S3 BUCKET
import pandas as pd
from io import BytesIO
import logging

from includes.transform_data import transform_data

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

# Define S3 bucket and file key
bucket_name = "vrams-travel-agency-bucket"
file_key = "raw_data/data.parquet"

def retrieve_and_process_data():
    """Retrieve a Parquet file from S3 and process it into a Pandas DataFrame."""
    try:
        s3_client = create_session().client("s3")
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        df = pd.read_parquet(BytesIO(response["Body"].read()))
        logging.info(f"Successfully retrieved and read the Parquet file from s3://{bucket_name}/{file_key}")
        return df
    except Exception as e:
        logging.error(f"Error retrieving or processing the Parquet file: {e}")
        # raise
        return pd.DataFrame()

def save_parquet_to_s3():
    """Reads raw data from S3, transforms it, and saves the transformed data back to S3."""
    try:
        # Retrieve raw data from S3
        raw_df = retrieve_and_process_data()

        # Apply transformation using extract_country_info
        # transformed_df = raw_df.apply(extract_and_rename_columns, axis=1, result_type="expand")
        transformed_df = transform_data(raw_df)

        # Print transformed data to the terminal
        # print("\nTransformed Data Preview:")
        print(transformed_df.dtypes)
        max_length = transformed_df['country_code'].astype(str).apply(len).max()
        print(f"Max length in Parquet file: {max_length}")
        
        # Define S3 target path for transformed data
        processed_file_key = "processed_data/processed_data.parquet"
        s3_client = create_session().client("s3")

        # Convert DataFrame to bytes
        buffer = BytesIO()
        transformed_df.to_parquet(buffer, index=False, engine="pyarrow") # Edited here
        buffer.seek(0)

        # Upload file to S3
        s3_client.put_object(Bucket=bucket_name, Key=processed_file_key, Body=buffer.getvalue())

        logging.info(f"Processed data successfully saved to s3://{bucket_name}/{processed_file_key}")

    except Exception as e:
        logging.error(f"Error saving processed data to S3: {e}")
        raise

# Call the function to save processed data to S3
save_parquet_to_s3()

