import pandas as pd
import boto3
import s3fs
from sqlalchemy import create_engine
import pymysql

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
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=file_key_prefix)

file_name = None
for obj in response.get('Contents', []):
    key = obj['Key']
    if not key.endswith('/'):  # skip folders
        file_name = key
        break
f file_name:
    print("File name found:", file_name)
else:
    raise Exception(" No file found in S3 bucket!")

# Step 2: Read the gzipped CSV file from S3
fs = s3fs.S3FileSystem(key=aws_access_key, secret=aws_secret_key)
file_path = f's3://{bucket_name}/{file_name}'

# Using smart_open to speed up reading from S3
from smart_open import open

with open(file_path, 'rb') as file:
    df = pd.read_csv(file, compression='gzip')

print("Data preview:")
print(df.head())

# Step 3: Connect to RDS and create the database if it doesn't exist
conn = pymysql.connect(host=rds_host, user=rds_user, password=rds_password)
cursor = conn.cursor()
cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name};")
print(f" Database '{database_name}' created or already exists!")
cursor.close()
conn.close()

# Step 4: Connect to the created database
conn = pymysql.connect(host=rds_host, user=rds_user, password=rds_password, database=database_name)
cursor = conn.cursor()

# Step 5: Create the table if it doesn't exist
create_table_query = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    close DECIMAL(10,4),
    window_start DATETIME,
    window_end DATETIME,
    high DECIMAL(10,5),
    low DECIMAL(10,5),
    open DECIMAL(10,5),
    total_volume BIGINT
);
"""
cursor.execute(create_table_query)
print(f" Table '{table_name}' created or already exists!")
cursor.close()
conn.close()

# Step 6: Upload DataFrame into MySQL table using SQLAlchemy
# Convert window_start and window_end columns to datetime
df['window_start'] = pd.to_datetime(df['window_start'])
df['window_end'] = pd.to_datetime(df['window_end'])

# Create engine and upload using engine.connect() to handle the active connection
engine = create_engine(f'mysql+pymysql://{rds_user}:{rds_password}@{rds_host}/{database_name}')
with engine.connect() as connection:
    df.to_sql(name=table_name, con=connection, index=False, if_exists='append', method='multi')

print(f" Data uploaded successfully into table '{table_name}' inside database '{database_name}'!")

