from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, md5, concat_ws, when


spark = SparkSession.builder.appName("DataReconciliation").getOrCreate()

# df = spark.read.parquet("source_data_parquet").sort("id")
df = spark.read.option("header", "true").csv("source_data")

df.show()
spark.stop()