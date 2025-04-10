import os
import datetime
from jinja2 import Environment, FileSystemLoader

class DataQualityReport:
    def __init__(self, pipeline_name, results, output_folder="reports", template_folder="resources"):
        self.pipeline_name = pipeline_name
        self.results = results
        self.output_folder = output_folder
        self.template_folder = template_folder
        self.timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    def generate_report(self):
        # Ensure output folder exists
        os.makedirs(self.output_folder, exist_ok=True)

        # File name with pipeline name & timestamp
        file_name = f"{self.pipeline_name}_{self.timestamp}.html"
        file_path = os.path.join(self.output_folder, file_name)

        # Count summary
        total_tests = len(self.results)
        passed = sum(1 for result in self.results.values() if result == "Passed")
        failed = sum(1 for result in self.results.values() if result == "Failed")
        skipped = sum(1 for result in self.results.values() if result == "Skipped")

        # Convert results to tuple format (for Jinja2)
        formatted_results = [
            (check, status, "✅" if status == "Passed" else "❌" if status == "Failed" else "⏭️", 
             "pass" if status == "Passed" else "fail" if status == "Failed" else "skipped")
            for check, status in self.results.items()
        ]

        # Load Jinja2 Template
        env = Environment(loader=FileSystemLoader(self.template_folder))
        template = env.get_template("report_template.html")

        # Render Template
        html_content = template.render(
            pipeline_name=self.pipeline_name,
            timestamp=self.timestamp,
            total_tests=total_tests,
            passed=passed,
            failed=failed,
            skipped=skipped,
            results=formatted_results
        )

        # Write to file
        with open(file_path, "w") as file:
            file.write(html_content)

        print(f"✅ Report generated successfully: {file_path}")
        return file_path

# Example Usage
if __name__ == "__main__":
    sample_results = {
        "Schema Match": "Passed",
        "Row Count Match": "Failed",
        "Data Match": "Passed",
        "Checksum Match": "Skipped"
    }
    report = DataQualityReport("Customer_Data_Validation", sample_results)
    report.generate_report()
