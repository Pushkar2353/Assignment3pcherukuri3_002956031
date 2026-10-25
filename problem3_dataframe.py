from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, expr

# Create SparkSession
spark = SparkSession.builder \
    .appName("Workers DataFrame Average Salary") \
    .getOrCreate()

# Read workers_pyspark_df directly from Assignment3 database
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

print("\nOriginal Schema:")
df.printSchema()

# Remove comma from Salary and convert Salary to DOUBLE
df_numeric = df.withColumn(
    "SalaryNumeric",
    expr("try_cast(replace(Salary, ',', '') AS DOUBLE)")
)

print("\nData after Salary conversion:")
df_numeric.show(truncate=False)

# Group by Gender and Department
DF2 = df_numeric.groupBy(
    "Gender",
    "Department"
)

# Calculate average salary
DF3 = DF2.agg(
    avg("SalaryNumeric").alias("Average_Salary")
)

# Sort results
result = DF3.orderBy(
    "Gender",
    "Department"
)

print("\nAverage Salary Results - DataFrame:")
result.show(truncate=False)

spark.stop()
