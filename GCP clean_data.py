# main.py
import pandas as pd
import os
import io
from google.cloud import storage
import functions_framework

@functions_framework.http
def clean_and_store_data(request):
    print("Starting data cleaning process for GCS files...")

    storage_client = storage.Client()

    request_json = request.get_json(silent=True)
    request_args = request.args

    bucket_name = 'run-sources-st-project-464315-asia-southeast1'
    source_file_name = 'raw_data.csv'
    cleaned_file_name = 'cleaned_data.csv'

    if request_json:
        bucket_name = request_json.get('bucket_name')
        source_file_name = request_json.get('source_file_name')
        cleaned_file_name = request_json.get('cleaned_file_name')
    elif request_args:
        bucket_name = request_args.get('bucket_name')
        source_file_name = request_args.get('source_file_name')
        cleaned_file_name = request_args.get('cleaned_file_name')

    if not all([bucket_name, source_file_name, cleaned_file_name]):
        error_message = "Missing required parameters: 'bucket_name', 'source_file_name', and 'cleaned_file_name' must be provided in the request body or query parameters."
        print(f"Error: {error_message}")
        return error_message, 400

    print(f"Processing file '{source_file_name}' from bucket '{bucket_name}'...")

    try:
        bucket = storage_client.bucket(bucket_name)
        source_blob = bucket.blob(source_file_name)

        csv_data = source_blob.download_as_bytes()
        df = pd.read_csv(io.BytesIO(csv_data))
        print(f"Successfully read {len(df)} rows from gs://{bucket_name}/{source_file_name}.")

        print("Applying data cleaning steps...")

        initial_rows = len(df)
        df.drop_duplicates(inplace=True)
        print(f"Removed {initial_rows - len(df)} duplicate rows. New row count: {len(df)}")

        print("\nMissing values before cleaning:")
        print(df.isnull().sum())

        df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
        print("\nAfter ensuring 'Price' is numeric and coercing errors:")
        print(df.isnull().sum())

        initial_price_nans = df['Price'].isnull().sum()
        if initial_price_nans > 0:
            df.dropna(subset=['Price'], inplace=True)
            print(f"Removed {initial_price_nans} rows due to unparseable 'Price' values.")

        initial_rows = len(df)
        df = df[df['Price'] > 0]
        print(f"Removed {initial_rows - len(df)} rows with Price <= 0. New row count: {len(df)}")

        df['Title'] = df['Title'].str.strip()
        print("Applied text cleaning (strip whitespace) to 'Title' column.")

        cleaned_blob = bucket.blob(cleaned_file_name)
        cleaned_csv_in_memory = io.StringIO()
        df.to_csv(cleaned_csv_in_memory, index=False, encoding='utf-8')
        cleaned_blob.upload_from_string(cleaned_csv_in_memory.getvalue(), content_type='text/csv')

        print(f"Cleaned data successfully saved to gs://{bucket_name}/{cleaned_file_name}")
        return "Data cleaning successful!", 200

    except Exception as e:
        print(f"An error occurred during data cleaning or storage: {e}")
        return f"Data cleaning failed: {e}", 500
