import pandas as pd
import os
import json
import re

def clean_data(client, bucket_name='datalake-case', raw_prefix='bronze_layer/raw', cleaned_dir='bronze_layer/cleaned'):
    """
    Clean raw JSON data from the specified MinIO bucket and save cleaned files locally.
    Replaces spaces with underscores in column names and data values.

    Args:
        client (boto3.client): The Boto3 client configured for MinIO.
        bucket_name (str): MinIO bucket name.
        raw_prefix (str): Prefix of the raw layer folder in the bucket.
        cleaned_dir (str): Local directory to store cleaned JSON files.
    """
    try:
        # Ensure the cleaned directory exists
        os.makedirs("tmp/cleaned", exist_ok=True)

        # List all files in the raw layer
        response = client.list_objects_v2(Bucket=bucket_name, Prefix=raw_prefix)
        if 'Contents' not in response:
            raise ValueError(f"No files found in the raw layer: {raw_prefix}")

        for obj in response['Contents']:
            file_key = obj['Key']
            if not file_key.endswith('.json'):
                continue  # Skip non-JSON files
            
            # Download the JSON file
            file_obj = client.get_object(Bucket=bucket_name, Key=file_key)
            breweries = json.load(file_obj['Body'])

            # Convert JSON data into a Pandas DataFrame
            df = pd.json_normalize(breweries)

            # Replace spaces in column names with underscores
            df.columns = [col.replace(' ', '_') for col in df.columns]

            # Replace spaces in data values with underscores and convert to lowercase
            # Use `map` to replace spaces in data values with underscores and convert to lowercase
            for column in df.select_dtypes(include='object').columns:  # Apply `map` only to string columns
                df[column] = df[column].map(
                    lambda x: re.sub(r'\s+', '_', str(x).lower()) if isinstance(x, str) else x
                )

            # Replace null values with 'Unknown'
            df.fillna('unknown', inplace=True)

            # Save cleaned data locally
            cleaned_file_path = os.path.join("/tmp/", os.path.basename(file_key))
            df.to_json(cleaned_file_path, orient='records')
            print(f"Cleaned data saved locally: {cleaned_file_path}")

            try:
                client.upload_file(cleaned_file_path, bucket_name, f'bronze_layer/cleaned/{os.path.basename(file_key)}')
                print(f"File {file_key} uploaded successfully to {bucket_name}/bronze_layer/cleaned/")
            except Exception as e:
                print(f"Error uploading file: {e}")
                raise

        print("All raw data cleaned and saved successfully.")
    except Exception as e:
        print(f"Error cleaning data: {e}")
        raise
