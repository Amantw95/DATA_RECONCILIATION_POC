# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, count, sum, avg, md5, concat_ws, when


# spark = SparkSession.builder.appName("DataReconciliation").getOrCreate()

# # df = spark.read.parquet("source_data_parquet").sort("id")
# df = spark.read.option("header", "true").csv("source_data")

# df.show()
# spark.stop()

import os

template_path = os.path.join("resources", "report_template.html")
if not os.path.exists(template_path):
    print(f"❌ Template file not found: {template_path}")
else:
    print(f"✅ Template found at: {template_path}")
