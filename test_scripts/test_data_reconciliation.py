import os
import sys
import pytest
from pyspark.sql import SparkSession

# Make sure the utilities folder is in the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utilities.data_reconciliation import DataReconciliation
from utilities.results_store import store_results
from utilities.generate_reports import DataQualityReport
from utilities.config_loader import load_config

# ----------- Fixtures ----------- #
@pytest.fixture(scope="module")
def spark():
    spark_session = SparkSession.builder.appName("DataReconciliation").getOrCreate()
    yield spark_session
    spark_session.stop()

@pytest.fixture(scope="module")
def config(request):
    config_name = request.config.getoption("--config-name")
    cfg = load_config(config_name)
    print(f"✅ Loaded Config for Pipeline: {config_name}")
    return cfg

# ----------- Test ----------- #
def test_data_reconciliation(spark, config):
    reconciler = DataReconciliation(config, spark)
    checks_result = reconciler.run_reconciliation()

    assert isinstance(checks_result, dict)
    for check, result in checks_result.items():
        assert result is not None

    store_results(config["name"], checks_result)

    report = DataQualityReport(config["name"], checks_result)
    path = report.generate_report()

    print(f"✅ Reconciliation Test Report saved at: {path}")
    assert os.path.exists(path)
