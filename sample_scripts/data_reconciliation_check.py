from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, md5, concat_ws, when

# Initialize PySpark
spark = SparkSession.builder.appName("DataReconciliation").getOrCreate()

# Load CSV files into DataFrames
df_source = spark.read.csv("source.csv", header=True, inferSchema=True)
df_target = spark.read.csv("target.csv", header=True, inferSchema=True)

# ✅ Row Count Validation
row_count_source = df_source.count()
row_count_target = df_target.count()
print(f"Row Count - Source: {row_count_source}, Target: {row_count_target}")

# ✅ Checksum Validation (MD5 Hash of concatenated columns)
df_source = df_source.withColumn("checksum", md5(concat_ws(",", *df_source.columns)))
df_target = df_target.withColumn("checksum", md5(concat_ws(",", *df_target.columns)))

checksum_diff = df_source.join(df_target, "id", "full_outer").filter(df_source.checksum != df_target.checksum)
print(f"Rows with checksum mismatch: {checksum_diff.count()}")

df_diff = df_source.alias("src").join(
    df_target.alias("tgt"), "id", "inner"
).filter(col("src.checksum") != col("tgt.checksum"))

# <----- Real Data Quality Use Case ------>
df_diff.select(
    [col(f"src.{c}").alias(f"src_{c}") for c in df_source.columns] +
    [col(f"tgt.{c}").alias(f"tgt_{c}") for c in df_target.columns]
).show(5, False)

# df_source.select("id", "checksum").show(5, False)
# df_target.select("id", "checksum").show(5, False)




# # ✅ Field-Level Comparison
for column in df_source.columns:
    mismatches = df_source.join(df_target, "id", "full_outer").filter(df_source[column] != df_target[column])
    print(f"Mismatches in column '{column}': {mismatches.count()}")

# # ✅ Null Value Check
null_check = df_source.select([count(when(col(c).isNull(), c)).alias(c) for c in df_source.columns])
null_check.show()

# # ✅ Duplicate Check
duplicate_source = df_source.groupBy(*df_source.columns).count().filter("count > 1")
duplicate_target = df_target.groupBy(*df_target.columns).count().filter("count > 1")

print(f"Duplicates in Source: {duplicate_source.count()}, Target: {duplicate_target.count()}")

# # ✅ Aggregate Validation
agg_source = df_source.agg(sum("salary").alias("total_salary"), avg("salary").alias("avg_salary"))
agg_target = df_target.agg(sum("salary").alias("total_salary"), avg("salary").alias("avg_salary"))

agg_source.show()
agg_target.show()

spark.stop()
