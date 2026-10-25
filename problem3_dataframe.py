from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, regexp_replace

# Create SparkSession
spark = SparkSession.builder \
    .appName("Workers DataFrame Average Salary") \
    .getOrCreate()

# Read workers_pyspark_df from Assignment3 MariaDB database
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

print("\n=== ORIGINAL DATABASE DATA ===")
df.show(20, truncate=False)

print("\n=== ORIGINAL SCHEMA ===")
df.printSchema()

# Convert Salary: "120,000" -> 120000
df2 = df.withColumn(
    "SalaryNumeric",
    regexp_replace(
        col("Salary"),
        ",",
        ""
    ).cast("double")
)

print("\n=== DATA AFTER SALARY CONVERSION ===")
df2.show(20, truncate=False)

# Calculate average salary for each Gender + Department combination
result = df2.groupBy(
    "Gender",
    "Department"
).agg(
    avg("SalaryNumeric").alias("Average_Salary")
)

print("\n=== AVERAGE SALARY RESULTS ===")
result.orderBy(
    "Gender",
    "Department"
).show(truncate=False)

spark.stop()
