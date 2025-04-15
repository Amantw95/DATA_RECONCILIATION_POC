import pandas as pd
import numpy as np
from faker import Faker

fake = Faker()
num_records = 1_000_000  # 1 million rows

# Define a random schema
schema = {
    "id": [fake.uuid4() for _ in range(num_records)],
    "name": [fake.name() for _ in range(num_records)],
    "email": [fake.email() for _ in range(num_records)],
    "age": np.random.randint(18, 80, num_records),
    "salary": np.round(np.random.uniform(30000, 150000, num_records), 2),
    "date_of_joining": [fake.date_this_decade() for _ in range(num_records)],
}

# Create a DataFrame
df = pd.DataFrame(schema)

# Save two slightly different versions for comparison
df.to_csv("source.csv", index=False)

# Introduce some changes in the target file
df.iloc[1000:1050, df.columns.get_loc("salary")] += 5000  # Modify some salaries
df.iloc[2000:2050, df.columns.get_loc("email")] = "changed_email@example.com"  # Modify some emails
df.iloc[3000:3050] = None  # Introduce some null values

df.to_csv("target.csv", index=False)

print("CSV files generated successfully!")