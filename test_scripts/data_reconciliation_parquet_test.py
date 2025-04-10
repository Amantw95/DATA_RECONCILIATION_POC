import sys,os
import pytest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utilities.data_reconciliation import DataReconciliation

# Path to the config file (update this based on your config file name & location)
CONFIG_FILE = "pipeline_parquet"

if __name__ == "__main__":
    # Initialize DataReconciliation with config file
    reconciler = DataReconciliation(CONFIG_FILE)

    # Run the reconciliation checks
    reconciler.run_reconciliation()
