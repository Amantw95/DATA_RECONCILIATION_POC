from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from scripts.config_loader import load_config
import os

# Load Configuration
config = load_config("pipeline1")

# Initialize Spark
spark = SparkSession.builder.master(config["spark_master"]).appName("Data Reconciliation").getOrCreate()

def validate_file_names(source_path, target_path):
    """
    Ensures both source and target directories have the same files.
    """
    source_files = set(os.listdir(source_path))
    target_files = set(os.listdir(target_path))

    if source_files != target_files:
        missing_source = target_files - source_files
        missing_target = source_files - target_files

        print("❌ File Name Mismatch Detected!")
        if missing_source:
            print(f"🚨 Missing in Source: {missing_source}")
        if missing_target:
            print(f"🚨 Missing in Target: {missing_target}")
        raise ValueError("❌ File Name Mismatch! Please check source and target files.")

# Validate file names before loading data
validate_file_names(config["source_path"], config["target_path"])

def load_data(file_path, file_type):
    """Loads data as a Spark DataFrame."""
    full_path = os.path.join(file_path)
    if file_type == "csv":
        return spark.read.option("header", "true").csv(full_path)
    elif file_type == "parquet":
        return spark.read.parquet(full_path)
    else:
        raise ValueError(f"❌ Unsupported file type: {file_type}")

# Load Source & Target Data
source_df = load_data(config["source_path"], config["file_type"])
target_df = load_data(config["target_path"], config["file_type"])

def validate_schema(source_df, target_df):
    """Checks if column names and data types match."""
    source_schema = set(source_df.dtypes)
    target_schema = set(target_df.dtypes)

    if source_schema != target_schema:
        raise ValueError(f"❌ Schema Mismatch!\nSource: {source_schema}\nTarget: {target_schema}")
    print("✅ Schema Validation Passed.")

if config["columns_check"]:
    validate_schema(source_df, target_df)

def validate_row_count(source_df, target_df):
    """Ensures both files have the same number of rows."""
    source_count = source_df.count()
    target_count = target_df.count()

    if source_count != target_count:
        raise ValueError(f"❌ Row Count Mismatch! Source: {source_count}, Target: {target_count}")
    print("✅ Row Count Validation Passed.")

if config["row_count_check"]:
    validate_row_count(source_df, target_df)

def reconcile_data(source_df, target_df):
    """
    Ensures data in both source and target files is identical.
    1. Checks for missing rows in either dataset.
    2. Ensures column order doesn't affect comparison.
    """
    # Standardize column order for comparison
    common_columns = sorted(source_df.columns)  # Get all common columns in sorted order
    source_df = source_df.select(common_columns)  # Reorder columns in source
    target_df = target_df.select(common_columns)  # Reorder columns in target

    # Find differences (PySpark exceptAll checks for exact matches)
    diff_source = source_df.exceptAll(target_df)
    diff_target = target_df.exceptAll(source_df)

    if diff_source.count() > 0 or diff_target.count() > 0:
        print("❌ Data Mismatch Found! Differences:")
        if diff_source.count() > 0:
            print("🚨 Rows in Source but NOT in Target:")
            diff_source.show(truncate=False)
        if diff_target.count() > 0:
            print("🚨 Rows in Target but NOT in Source:")
            diff_target.show(truncate=False)
    else:
        print("✅ Data Reconciliation Passed. No Differences Found.")

# Run Reconciliation if enabled
if config["data_reconciliation"]:
    reconcile_data(source_df, target_df)