import libraries
import pandas as pd
import boto3
import s3fs
import pymysql
from sqlalchemy import create_engine

# AWS credentials
aws_access_key = 'AKIAUX6GOXZZB742GA7V'
aws_secret_key = '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'

# S3 bucket and prefix
bucket_name = 'myprojectbuckk-rrr'
file_key_prefix = 'output/'

# RDS MySQL connection details
rds_host = 'roobandb1.ctoeqy6m8a6l.ap-south-1.rds.amazonaws.com'
rds_user = 'admin'
rds_password = 'Admin123'

# Database and table names
database_name = 'new_database'
table_name = 'new_table'

# Step 1: Find the latest file in S3
s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)
esponse = s3.list_objects_v2(Bucket=bucket_name, Prefix=file_key_prefix)

file_name = None
for obj in response.get('Contents', []):
    key = obj['Key']
    if not key.endswith('/'):  # skip folders
        file_name = key
        break

if file_name:
    print("✅ File name found:", file_name)
else:
    raise Exception("⚠️ No file found in S3 bucket!")

# Step 2: Read the gzipped CSV file from S3
fs = s3fs.S3FileSystem(key=aws_access_key, secret=aws_secret_key)
file_path = f's3://{bucket_name}/{file_name}'

df = pd.read_csv(file_path, compression='gzip', storage_options={'key': aws_access_key, 'secret': aws_secret_key})
print("✅ Data preview:")
print(df.head())

# Step 3: Connect to RDS and create the database if it doesn't exist
conn = pymysql.connect(host=rds_host, user=rds_user, password=rds_password)
cursor = conn.cursor()
cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name};")
 Get connection from engine
with engine.connect() as connection:
    # Start a transaction
    with connection.begin():
        # Insert data row by row (to avoid errors with large data sets)
        for _, row in df.iterrows():
            insert_query = f"""
            INSERT INTO {table_name} (close, window_start, window_end, high, low, open, total_volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            connection.execute(insert_query, tuple(row))

print(f"✅ Data uploaded successfully into table '{table_name}' inside database '{database_name}'!")

