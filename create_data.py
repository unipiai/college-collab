import pandas as pd
import numpy as np

# Create sample CSV data with 1000 rows and 10 columns
np.random.seed(42)

# Columns: ID, Name, Department, Country, Age, Salary, YearsExperience, Rating, Projects, JoiningDate
num_rows = 1000
departments = ["Engineering", "Sales", "HR", "Finance", "Marketing", "Support"]
countries = ["USA", "India", "UK", "Germany", "Canada", "Australia"]
names = [f"Employee_{i}" for i in range(num_rows)]

data = {
    "EmployeeID": range(1, num_rows + 1),
    "Name": names,
    "Department": np.random.choice(departments, num_rows),
    "Country": np.random.choice(countries, num_rows),
    "Age": np.random.randint(22, 60, num_rows),
    "Salary": np.random.randint(40000, 150000, num_rows),
    "YearsExperience": np.random.randint(1, 25, num_rows),
    "Rating": np.round(np.random.uniform(1, 5, num_rows), 1),
    "Projects": np.random.randint(1, 10, num_rows),
    "JoiningDate": pd.to_datetime("2010-01-01") + pd.to_timedelta(np.random.randint(0, 5000, num_rows), unit="D")
}

df = pd.DataFrame(data)

# Save CSV
csv_path = "sample_employees.csv"
df.to_csv(csv_path, index=False)

# Create a config.yaml file describing the columns
config_content = """
columns:
  EmployeeID: Unique identifier for each employee.
  Name: Full name of the employee.
  Department: Department where the employee works.
  Country: Country of the employee's work location.
  Age: Age of the employee in years.
  Salary: Annual salary of the employee in USD.
  YearsExperience: Total years of professional experience.
  Rating: Average performance rating between 1 to 5.
  Projects: Number of projects handled by the employee.
  JoiningDate: The date when the employee joined the company.
"""

config_path = "config.yaml"
with open(config_path, "w") as f:
    f.write(config_content)

(csv_path, config_path)
