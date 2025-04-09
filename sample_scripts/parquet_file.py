import pandas as pd

# Create additional data
new_data = {
    "id": [16, 17, 18, 19, 20],
    "name": ["Kevin", "Liam", "Mia", "Noah", "Olivia"],
    "age": [28, 32, 27, 35, 30],
    "city": ["Houston", "Miami", "Chicago", "Austin", "Phoenix"]
}

# Convert to DataFrame
new_df = pd.DataFrame(new_data)

# Append to existing Parquet file
new_df.to_parquet("sample.parquet", engine="pyarrow")

print("✅ New data (ID 16-20) added to Parquet file!")
