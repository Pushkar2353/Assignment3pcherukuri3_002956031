import json
import ray
import mysql.connector

# Connect to the existing Ray cluster
ray.init(address="auto")

# Load the JSON array from workers.json
with open("/home/pcherukuri3/Assignment3/workers.json", "r") as file:
    workers = json.load(file)

# Create a Ray Dataset
dataset = ray.data.from_items(workers)

# Keep the columns in the same order as the MariaDB table
dataset = dataset.select_columns(
    ["Department", "Gender", "Name", "Salary"]
)

print("Workers loaded into Ray Dataset:")
dataset.show(12)

print("Ray Dataset schema:")
print(dataset.schema())

# MariaDB connection
def create_connection():
    return mysql.connector.connect(
        user="dbadmin",
        password="testpassword",
        host="192.168.1.5",
        port=3306,
        database="Assignment3",
        connection_timeout=30,
    )

# Save Ray Dataset into MariaDB
dataset.write_sql(
    """
    INSERT INTO workers_ray_ds
    (Department, Gender, Name, Salary)
    VALUES (%s, %s, %s, %s)
    """,
    create_connection,
    concurrency=1,
)

print("Successfully saved Ray Dataset to workers_ray_ds")
print("Number of rows:", dataset.count())

ray.shutdown()
