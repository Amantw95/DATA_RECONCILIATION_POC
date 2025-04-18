import pandas as pd
import os
from datetime import datetime

def store_results(pipeline_name, check_results, output_file=None):
    """Generic function to store reconciliation results dynamically for any number of checks."""

    # Generate dynamic filename with timestamp if not provided
    if output_file is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_pipeline_name = pipeline_name.replace(" ", "_")  # Avoid spaces
        output_file = f"reconciliation_results_{safe_pipeline_name}_{timestamp}.csv"

    # Separate checks by their status
    passed_checks = [check for check, result in check_results.items() if str(result).lower() == "passed"]
    failed_checks = [check for check, result in check_results.items() if str(result).lower() == "failed"]
    skipped_checks = [check for check, result in check_results.items() if str(result).lower() == "skipped"]

    # Convert lists to strings
    passed_checks_str = ", ".join(passed_checks) if passed_checks else "None"
    failed_checks_str = ", ".join(failed_checks) if failed_checks else "None"
    skipped_checks_str = ", ".join(skipped_checks) if skipped_checks else "None"

    # Create result dictionary
    result_data = {
        "Pipeline": pipeline_name,
        "Passed Checks": passed_checks_str,
        "Failed Checks": failed_checks_str,
        "Skipped Checks": skipped_checks_str,
    }

    # Add check results with emoji
    for check_name, result in check_results.items():
        result_lower = str(result).lower()
        if result_lower == "passed":
            emoji = "✅"
        elif result_lower == "failed":
            emoji = "❌"
        elif result_lower == "skipped":
            emoji = "⏭️"
        else:
            emoji = "❓"
        result_data[check_name] = emoji

    # Convert dictionary to DataFrame
    df = pd.DataFrame([result_data])

    # Define results directory
    output_dir = "results"
    os.makedirs(output_dir, exist_ok=True)
    file_path = os.path.join(output_dir, output_file)

    # Write the new file
    df.to_csv(file_path, index=False)

    print(f"📄 Results saved to: {file_path}")
