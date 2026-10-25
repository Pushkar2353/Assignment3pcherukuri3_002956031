import ray
import mysql.connector

from ray.data.aggregate import Mean


# Connect to the existing Ray cluster
ray.init(address="auto")


# Function used by Ray Data to connect to MariaDB
def create_connection():
    return mysql.connector.connect(
        user="dbadmin",
        password="testpassword",
        host="192.168.1.5",
        port=3306,
        database="Assignment3",
        connection_timeout=30,
    )


# Load workers_ray_ds directly from MariaDB
dataset = ray.data.read_sql(
    """
    SELECT Department, Gender, Name, Salary
    FROM workers_ray_ds
    """,
    create_connection,
)

print("\n=== DATA LOADED FROM workers_ray_ds ===")
dataset.show(12)

print("\n=== ORIGINAL RAY DATASET SCHEMA ===")
print(dataset.schema())


# Convert Salary from strings such as "120,000"
# into numeric values such as 120000.0
def convert_salary(row):
    return {
        "Department": row["Department"],
        "Gender": row["Gender"],
        "Name": row["Name"],
        "SalaryNumeric": float(
            row["Salary"].replace(",", "")
        ),
    }


numeric_dataset = dataset.map(convert_salary)

print("\n=== DATA AFTER SALARY CONVERSION ===")
numeric_dataset.show(12)

print("\n=== NUMERIC DATASET SCHEMA ===")
print(numeric_dataset.schema())


# Group by both Gender and Department
# and calculate mean salary
result = (
    numeric_dataset
    .groupby(["Gender", "Department"])
    .aggregate(
        Mean(
            on="SalaryNumeric",
            alias_name="Average_Salary"
        )
    )
)

print("\n=== AVERAGE SALARY RESULTS - RAY DATA ===")
result.show(10)


ray.shutdown()
