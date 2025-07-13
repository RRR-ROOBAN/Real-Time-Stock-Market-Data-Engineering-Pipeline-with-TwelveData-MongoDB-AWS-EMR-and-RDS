                                                                                              
import requests
import pymongo
from datetime import datetime
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('Practise').getOrCreate()
import boto3
import os

# 1. Fetch Data
apikey = "648fa098b827452e9c1073a19a48842c"
params = {
    "symbol": "AAPL",
    "interval": "5min",
    "outputsize": 1500,
    "apikey": apikey,
    "format": "JSON"
}

res = requests.get("https://api.twelvedata.com/time_series", params=params)
data = res.json()

# 2. Process data
data_dict_list = []
for record in data.get("values", []):
    data_dict_list.append({
        "datetime": record["datetime"],
        "open": record["open"],
        "high": record["high"],
        "low": record["low"],
        "close": record["close"],
        "volume": record["volume"]
    })

# 3. Upload to MongoDB
Mongo_Username = "Roobanruban"
Mongo_Password = "Roobanatlas"
Mongo_NewDB = "GUVI_Project"
Mongo_Collection = "twelvedata"

client = pymongo.MongoClient(
    f"mongodb+srv://{Mongo_Username}:{Mongo_Password}@cluster0.6saiv.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
)
db = client[Mongo_NewDB]
collection = db[Mongo_Collection]
collection.insert_many(data_dict_list)

#4.convert to Dataframe

mongo_data = list(collection.find({}, {'_id': 0}))
df=spark.createDataFrame(mongo_data)

# Step 5: Add sl_no using row_number()
#windowSpec = Window.orderBy(df["datetime"].desc())
#df_with_slno = df.withColumn("sl_no", row_number().over(windowSpec))


#cols = df_with_slno.columns
#reordered_cols = ["sl_no"] + [col for col in cols if col != "sl_no"]
#df_final = df_with_slno.select(reordered_cols)

# step 6 : df to Dataframe
df.coalesce(1).write.mode("overwrite").option("header", True).csv("/home/vboxuser/final_csv")

# step 7 : Upload to csv to s3

s3 = boto3.client(
    "s3",
   aws_access_key_id="AKIAUX6GOXZZB742GA7V",
   aws_secret_access_key="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",
)


bucketname="myprojectbuckk-rrr"
s3_key="final/final_output.csv"

path="/home/vboxuser/final_csv"


filename=[f for f in os.listdir(path) if f.startswith("part-") and f.endswith(".csv")]
full_path=os.path.join(path,filename[0])


s3.upload_file(Filename=full_path, Bucket=bucketname, Key=s3_key)



# Initialize the Glue client for the Mumbai region (ap-south-1)
client = boto3.client('glue', region_name='ap-south-1')

# Trigger the Glue job named 'Myetl'
response = client.start_job_run(
    JobName='Myetl',  # Your Glue job name
)

# Print the job run ID to track it
print("Job Run ID:", response['JobRunId'])




# Check the status of the Glue job
job_run = client.get_job_run(
    JobName='Myetl',  # Your Glue job name
    RunId=response['JobRunId']  # Use the JobRunId from the previous step
)

# Print the job status
print("Job Status:", job_run['JobRun']['JobRunState'])

