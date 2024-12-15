import os
import json
import pandas as pd
import boto3
import io

def create_bronze_layer(client, breweries, bucket_name='datalake-case', file_name='bronze_breweries.json'):
    """
    Upload raw brewery data to MinIO bucket as a JSON file.
    
    Args:
        client (boto3.client): The Boto3 client configured for MinIO
        breweries (list): List of brewery data.
        bucket_name (str): MinIO bucket name.
        file_name (str): File name to save in the bucket.
    """

    # Save data to a temporary file
    file_path = f"/tmp/{file_name}"
    os.makedirs(os.path.dirname(file_path), exist_ok=True) 
    with open(file_path, "w") as f:
        json.dump(breweries, f)

    try:
        client.upload_file(file_path, bucket_name, f'/bronze_layer/raw/{file_name}')
        print(f"File {file_name} uploaded successfully to {bucket_name}.")
    except Exception as e:
        print(f"Error uploading file: {e}")
        raise

def create_silver_layer(client, bucket_name='datalake-case', bronze_cleaned_prefix='bronze_layer/cleaned', silver_dir='silver_layer/'):
    """
    Transform raw brewery data from the bronze layer (cleaned) to columnar storage (Parquet) and partition by state.
    Save the transformed files locally in the Docker container under `/tmp/`.

    Args:
        client (boto3.client): The Boto3 client configured for MinIO.
        bucket_name (str): MinIO bucket name.
        bronze_cleaned_prefix (str): Prefix of the cleaned bronze layer folder in the bucket.
        silver_dir (str): Local directory to store transformed Parquet files.
    """
    try:
        # List all files in the cleaned bronze layer
        response = client.list_objects_v2(Bucket=bucket_name, Prefix=bronze_cleaned_prefix)
        if 'Contents' not in response:
            raise ValueError(f"No files found in the cleaned bronze layer: {bronze_cleaned_prefix}")
        
        df_list = []

        for obj in response['Contents']:
            file_key = obj['Key']
            if not file_key.endswith('.json'):
                continue  # Skip non-JSON files
            
            # Download the JSON file
            file_obj = client.get_object(Bucket=bucket_name, Key=file_key)
            breweries = json.load(file_obj['Body'])
            
            # Convert the JSON list to a Pandas DataFrame
            df = pd.DataFrame(breweries)

            # Check if 'state' column exists for partitioning
            if 'state' not in df.columns:
                raise ValueError(f"'state' column is missing in the file: {file_key}")
            
            # Append the DataFrame to the list
            df_list.append(df)

        # Concatenate all DataFrames in df_list into a single DataFrame
        final_df = pd.concat(df_list, ignore_index=True)

        # Partition data by 'state' and save as Parquet
        for state, partition_df in final_df.groupby('state'):
            # Create directory structure for partition
            partition_path = os.path.join('/tmp/', state)
            os.makedirs(partition_path, exist_ok=True)
            
            # Generate file path for partitioned data
            partition_file_name = f"breweries_{state}.parquet"
            partition_file_path = os.path.join(partition_path, partition_file_name)
            
            # Save the partitioned data locally as Parquet
            partition_df.to_parquet(partition_file_path, index=False)
            print(f"Partition saved locally: {partition_file_path}")
            
            # Upload the partition to MinIO
            try:
                # Prepare the S3 key (path) for MinIO
                s3_key = f"{silver_dir}{state}/{os.path.basename(partition_file_path)}"
                client.upload_file(partition_file_path, bucket_name, s3_key)
                print(f"Partition {s3_key} uploaded successfully to {bucket_name}.")
            except Exception as e:
                print(f"Error uploading partition {state}: {e}")
                raise

        print("Silver layer transformation and local storage completed successfully.")
    except Exception as e:
        print(f"Error in silver layer processing: {e}")
        raise

def create_gold_layer(client, bucket_name='datalake-case', silver_dir='silver_layer/', gold_dir='golden_layer/'):
    """
    Create an aggregated view of the number of breweries per type and location.
    The aggregated data is saved as Parquet file in the Gold Layer.

    Args:
        client (boto3.client): The Boto3 client configured for MinIO.
        bucket_name (str): MinIO bucket name.
        silver_dir (str): The directory in the bucket where the silver layer files are stored.
        gold_dir (str): Local directory to store the aggregated files for the Gold Layer (Parquet).
    """
    try:
        # List all files in the Silver layer
        response = client.list_objects_v2(Bucket=bucket_name, Prefix=silver_dir)
        if 'Contents' not in response:
            raise ValueError(f"No files found in the silver layer: {silver_dir}")

        # Prepare to aggregate data from Silver layer
        aggregated_data = []

        # Loop through each file in the Silver layer
        for obj in response['Contents']:
            file_key = obj['Key']
            
            # Skip files that are not Parquet
            if file_key.endswith('.parquet'):
                file_obj = client.get_object(Bucket=bucket_name, Key=file_key)
                
                # Read the content of the file into a BytesIO buffer
                parquet_file = io.BytesIO(file_obj['Body'].read())
                
                # Now load the file into a DataFrame
                df = pd.read_parquet(parquet_file)
            
            # Aggregating by brewery type and state (or location)
            if 'brewery_type' in df.columns and 'state' in df.columns:
                aggregated = df.groupby(['brewery_type', 'state']).size().reset_index(name='brewery_count')
                aggregated_data.append(aggregated)
            else:
                print(f"Skipping {file_key} as it doesn't contain 'brewery_type' or 'state' columns.")
                raise
        
        # Combine all aggregated data into a single DataFrame
        if aggregated_data:
            aggregated_df = pd.concat(aggregated_data, ignore_index=True)

            # Ensure the gold directory exists
            os.makedirs('/tmp/', exist_ok=True)

            # Save the aggregated data as a Parquet file
            gold_parquet_file_path = os.path.join('/tmp/', 'brewery_aggregated_by_type_and_location.parquet')
            aggregated_df.to_parquet(gold_parquet_file_path, index=False)
            print(f"Aggregated Parquet data saved locally: {gold_parquet_file_path}")

            # Upload the aggregated Parquet file to the Gold Layer in MinIO
            try:
                parquet_s3_key = f"{gold_dir}brewery_aggregated_by_type_and_location.parquet"
                client.upload_file(gold_parquet_file_path, bucket_name, parquet_s3_key)
                print(f"Aggregated Parquet uploaded successfully to {bucket_name}/{parquet_s3_key}")
            except Exception as e:
                print(f"Error uploading aggregated Parquet file: {e}")
                raise
        
        else:
            print("No valid aggregated data found.")
            raise
        
    except Exception as e:
        print(f"Error in gold layer processing: {e}")
        raise