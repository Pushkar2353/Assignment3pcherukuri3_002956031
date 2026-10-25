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
        "jdbc:mysql://192.168.1.5:3306/Assignment3?permitMysqlScheme"
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

# Convert salary from text such as "120,000" to 120000.0
df_numeric = df.withColumn(
    "SalaryNumeric",
    regexp_replace(
        col("Salary"),
        ",",
        ""
    ).cast("double")
)

print("\n=== DATA AFTER SALARY CONVERSION ===")
df_numeric.show(20, truncate=False)

# Calculate average salary for Gender + Department
result = df_numeric \
    .groupBy("Gender", "Department") \
    .agg(
        avg("SalaryNumeric").alias("Average_Salary")
    ) \
    .orderBy("Gender", "Department")

print("\n=== AVERAGE SALARY RESULTS ===")
result.show(truncate=False)

spark.stop()
