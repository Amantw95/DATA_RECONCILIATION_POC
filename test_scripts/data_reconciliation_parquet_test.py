import sys,os
import pytest
from pyspark.sql import SparkSession

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utilities.data_reconciliation import DataReconciliation
from utilities.results_store import store_results
from utilities.generate_reports import DataQualityReport
from utilities.config_loader import load_config

# Path to the config file (update this based on your config file name & location)
CONFIG_FILE = "pipeline_parquet"

if __name__ == "__main__":
    config = load_config(CONFIG_FILE)
    print(config)
    spark = SparkSession.builder.appName("DataReconciliation").getOrCreate()
    
    reconciler = DataReconciliation(config, spark)
    checks_result = reconciler.run_reconciliation()

    store_results(config["name"], checks_result)

    report = DataQualityReport(config["name"],checks_result)
    path = report.generate_report()
    print(f"✅ Reconciliation Test Report Save at :  {path}")


