import pandas as pd
import numpy as np
from faker import Faker
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

fake = Faker()
num_records = 1_000_000  # 1 million rows

# Define a random schema
schema = {
    "id": [fake.uuid4() for _ in range(num_records)],
    "name": [fake.name() for _ in range(num_records)],
    "email": [fake.email() for _ in range(num_records)],
    "age": np.random.randint(18, 80, num_records),
    "salary": np.round(np.random.uniform(30000, 150000, num_records), 2),
    "date_of_joining": [fake.date_this_decade() for _ in range(num_records)],
}

# Create a DataFrame
df = pd.DataFrame(schema)

# Save two slightly different versions for comparison
df.to_csv("source.csv", index=False)

# Introduce some changes in the target file
df.iloc[1000:1050, df.columns.get_loc("salary")] += 5000  # Modify some salaries
df.iloc[2000:2050, df.columns.get_loc("email")] = "changed_email@example.com"  # Modify some emails
df.iloc[3000:3050] = None  # Introduce some null values

df.to_csv("target.csv", index=False)

print("CSV files generated successfully!")

# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<


# Create a Spark session with your AWS Credentials
aws_access_key_id= 'ACCESS_KEY'
aws_secret_access_key = '<SECRET_KEY'
aws_session_token = 'SESSION_TOKEN'

conf = (
    SparkConf()
    .setAppName("MY_APP") # replace with your desired name
    # .set("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0,org.apache.hadoop:hadoop-aws:3.3.2")
    # .set("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .set("spark.jars.packages", 
         "org.apache.hadoop:hadoop-aws:3.2.0")
    .set("spark.hadoop.fs.s3a.aws.credentials.provider",
         "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
    .set("spark.hadoop.fs.s3a.access.key", aws_access_key_id)
    .set("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key)
    .set("spark.hadoop.fs.s3a.session.token", aws_session_token) # optional
    .set("spark.sql.shuffle.partitions", "4") # default is 200 partitions which is too many for local
    .setMaster("local[*]") # replace the * with your desired number of cores. * for use all.
)

spark = SparkSession.builder.config(conf=conf).getOrCreate()
df = spark.read.parquet("s3a://<S3_BUCKET_NAME>")

# Show DataFrame
df.show()


# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

# Create additional data
new_data = {
    "id": [16, 17, 18, 19, 20],
    "name": ["Kevin", "Liam", "Mia", "Noah", "Olivia"],
    "age": [28, 32, 27, 35, 30],
    "city": ["Houston", "Miami", "Chicago", "Austin", "Phoenix"]
}

# Convert to DataFrame
new_df = pd.DataFrame(new_data)

# Append to existing Parquet file
new_df.to_parquet("sample.parquet", engine="pyarrow")

print("✅ New data (ID 16-20) added to Parquet file!")

#<<<<<<<<<<<<>>>>>>>>>>>>>>>>.
# Delta tables

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

builder = (
    SparkSession.builder
    .appName("DeltaExample")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# # Read CSV
csv_df = spark.read.option("header", True).csv("customer.csv")

# Write as Delta
csv_df.write.format("delta").mode("overwrite").save("customers_delta")

print("✅ Delta table created successfully")

delta_df = spark.read.format("delta").load("customers_delta")
delta_df.show()

