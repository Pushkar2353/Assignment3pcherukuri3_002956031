from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, regexp_replace

# Create SparkSession
spark = SparkSession.builder \
    .appName("Workers DataFrame Average Salary") \
    .getOrCreate()

# Read workers_pyspark_df directly from MariaDB
df = spark.read.format("jdbc") \
    .option(
        "url",
        "jdbc:mariadb://192.168.1.5:3306/Assignment3"
    ) \
    .option("dbtable", "workers_pyspark_df") \
    .option("user", "dbadmin") \
    .option("password", "testpassword") \
    .option("driver", "org.mariadb.jdbc.Driver") \
    .load()

print("\nData loaded from workers_pyspark_df:")
df.show(truncate=False)

# Convert Salary from "120,000" to 120000.0
df_numeric = df.withColumn(
    "SalaryNumeric",
    regexp_replace(col("Salary"), ",", "").cast("double")
)

# Compute average salary by Gender and Department
result = df_numeric \
    .groupBy("Gender", "Department") \
    .agg(
        avg("SalaryNumeric").alias("Average_Salary")
    ) \
    .orderBy("Gender", "Department")

print("\nAverage Salary Results - DataFrame:")
result.show(truncate=False)

spark.stop()
