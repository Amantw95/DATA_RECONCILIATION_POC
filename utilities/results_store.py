import pandas as pd
import os

def store_results(pipeline_name, check_results, output_file="reconciliation_results.csv"):
    """Generic function to store reconciliation results dynamically for any number of checks."""

    # Separate passed and failed checks dynamically
    passed_checks = [check for check, result in check_results.items() if result]
    failed_checks = [check for check, result in check_results.items() if not result]

    # Convert lists to strings
    passed_checks_str = ", ".join(passed_checks) if passed_checks else "None"
    failed_checks_str = ", ".join(failed_checks) if failed_checks else "None"

    # Create a structured result dictionary
    result_data = {
        "Pipeline": pipeline_name,
        "Passed Checks": passed_checks_str,
        "Failed Checks": failed_checks_str,
    }

    # Dynamically add all checks as separate columns
    for check_name, result in check_results.items():
        result_data[check_name] = "✅" if result else "❌"

    # Convert dictionary to DataFrame
    df = pd.DataFrame([result_data])

    # Define results directory
    output_dir = "results"
    os.makedirs(output_dir, exist_ok=True)
    file_path = os.path.join(output_dir, output_file)

    # Append results to CSV
    if os.path.exists(file_path):
        df.to_csv(file_path, mode='a', header=False, index=False)
    else:
        df.to_csv(file_path, index=False)

    print(f"📄 Results saved to: {file_path}")
