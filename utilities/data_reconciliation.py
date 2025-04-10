from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2, concat_ws, col
import os
from utilities.config_loader import load_config
from utilities.results_store import store_results
from utilities.generate_reports import DataQualityReport

class DataReconciliation:
    def __init__(self, config_file):
        self.config = load_config(config_file)
        self.spark = SparkSession.builder.appName("DataReconciliation").getOrCreate()
        self.check_results = {}

    def load_data(self, path, file_format):
        """Load data from the given path and format."""
        if file_format == "csv":
            return self.spark.read.option("header", "true").csv(path)
        elif file_format == "parquet":
            return self.spark.read.parquet(path)
        else:
             raise ValueError(f"❌ Unsupported file type: {file_format}")

    def validate_schema(self, df1, df2):
        """Check if both DataFrames have the same schema."""
        return "Passed" if set(df1.schema) == set(df2.schema) else "Failed"

    def compare_row_counts(self, df1, df2):
        """Check if both DataFrames have the same number of rows."""
        return "Passed" if df1.count() == df2.count() else "Failed"

    def reconcile_data(self, df1, df2):
        """Check if both DataFrames have the same data."""
        df1_sorted = df1.sort(df1.columns)
        df2_sorted = df2.sort(df2.columns)
        return "Passed" if df1_sorted.exceptAll(df2_sorted).count() == 0 else "Failed"
    
    def generate_checksum(self, df):
        """Generate a checksum for a large dataset in a distributed manner."""
        row_hashes = df.withColumn("row_hash", sha2(concat_ws(",", *df.columns), 256))
        checksum = row_hashes.selectExpr("sha2(concat_ws(',', collect_list(row_hash)), 256) as checksum").collect()[0]["checksum"]
        return checksum
    
    def validate_checksum(self, df1, df2):
        """Check if both DataFrames have the same Checksums."""
        return "Passed" if self.generate_checksum(df1) == self.generate_checksum(df2) else "Failed"

    def run_reconciliation(self):
        """Run the entire reconciliation process."""
        source_path = self.config["source"]["path"]
        target_path = self.config["target"]["path"]
        file_format = self.config["source"]["format"]

        # source_files = set(os.listdir(source_path))
        # target_files = set(os.listdir(target_path))

        # if source_files != target_files:
        #     print("❌ Source and target files do not match!")
        #     print("Source files:", source_files)
        #     print("Target files:", target_files)
        #     return

        # for file in source_files:

        source_df = self.load_data(source_path, file_format)
        target_df = self.load_data(target_path, file_format)

        source_df.show();
        target_df.show();

        if self.config["schema_check"]:
            self.check_results["schema_check"] = self.validate_schema(source_df, target_df)
        else:
            self.check_results["schema_check"] = "Skipped"

        if self.config["row_count_check"]:
            self.check_results["row_count_check"] = self.compare_row_counts(source_df, target_df)
        else:
            self.check_results["row_count_check"] = "Skipped"

        if self.config["data_reconciliation"]:
           self.check_results["data_reconciliation"] = self.reconcile_data(source_df, target_df)
        else:
            self.check_results["data_reconciliation"] = "Skipped"

        if self.config["checksum_check"]:
            self.check_results["checksum_check"] = self.validate_checksum(source_df, target_df)
        else:
            self.check_results["checksum_check"] = "Skipped"


        print(f"✅ Reconciliation Test Execution Completed")

        report = DataQualityReport(self.config["name"], self.check_results)
        path = report.generate_report()
        print(f"✅ Reconciliation Test Report Save at :  {path}")
            