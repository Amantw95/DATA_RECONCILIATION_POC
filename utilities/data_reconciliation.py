from pyspark.sql.functions import sha2, concat_ws, col

from utilities.logger import get_logger

logger = get_logger("DataReconciliation")

check_registry = {}

def register_check(name):
    def decorator(func):
        check_registry[name] = func
        return func
    return decorator

class DataReconciliation:
    def __init__(self, configs, spark_session):
        self.config = configs
        self.spark = spark_session
        self.check_results = {}

    def load_data(self, path, file_format):
        """Load data from the given path and format."""
        if file_format == "csv":
            return self.spark.read.option("header", "true").csv(path)
        elif file_format == "parquet":
            return self.spark.read.parquet(path)
        else:
             raise ValueError(f"❌ Unsupported file type: {file_format}")

    @register_check("schema_check")
    def validate_schema(self, df1, df2):
        """Check if both DataFrames have the same schema."""
        return "Passed" if set(df1.schema) == set(df2.schema) else "Failed"

    # @register_check("row_count_check")
    # def compare_row_counts(self, df1, df2):
    #     """Check if both DataFrames have the same number of rows."""
    #     return "Passed" if df1.count() == df2.count() else "Failed"
    
    @register_check("data_reconciliation")
    def reconcile_data(self, df1, df2):
        """Check if both DataFrames have the same data."""

        df1.count()
        df2.count()
      
        if df1.count() == df2.count():
        
        # Compare both DataFrames using exceptAll
            diff_df = df1.exceptAll(df2)

            if diff_df.count() == 0:
                print("No differences found between source and target DataFrames.")
                return "Passed"
            else:
                print("Differences found between source and target DataFrames:")
                diff_df.show(100, False)
                return "Failed"
        else:
            diff_df_source = df1.exceptAll(df2)
            diff_df_target = df2.exceptAll(df1)    
            print("Differences found between source and target DataFrames:")
            diff_df_source.show(100, False)
            diff_df_target.show(100, False)
            return "Failed"
    
    #optional
    # def generate_checksum(self, df):
    #     """Generate a checksum for a large dataset in a distributed manner."""
    #     row_hashes = df.withColumn("row_hash", sha2(concat_ws(",", *df.columns), 256))
    #     checksum = row_hashes.selectExpr("sha2(concat_ws(',', collect_list(row_hash)), 256) as checksum").collect()[0]["checksum"]
    #     return checksum
    
    #optional
    # @register_check("checksum_check")
    # def validate_checksum(self, df1, df2):
    #     """Check if both DataFrames have the same Checksums."""
    #     return "Passed" if self.generate_checksum(df1) == self.generate_checksum(df2) else "Failed"

    def run_reconciliation(self):
        """Run the entire reconciliation process."""
    
        source_path = self.config["source"]["path"]
        target_path = self.config["target"]["path"]
        file_format = self.config["source"]["format"]
        logger.debug("<<<<<< Extracted Configs from yaml Files >>>>>>")

        source_df = self.load_data(source_path, file_format).cache()
        target_df = self.load_data(target_path, file_format).cache()

        # source_df.show();
        # target_df.show();


        for check, method in check_registry.items():
            if self.config.get(check):
                logger.debug(f"✔️ Running {check}")
                try:
                    self.check_results[check] = method(self,source_df, target_df)
                except Exception as e:
                    logger.error(f"❌ Error during {check}", exc_info=True)

            else:
                logger.debug(f"⏭️ {check} is skipped.")
                self.check_results[check] = "Skipped"

        logger.info("🏁 Reconciliation complete.")
        return self.check_results
    
            