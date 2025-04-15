# conftest.py
def pytest_addoption(parser):
    parser.addoption(
        "--config-name",
        action="store",
        default="pipeline_csv",
        help="Name of the config to run data reconciliation"
    )
