from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
    .appName("Workers Spark SQL Average Salary") \
    .getOrCreate()

# Read workers_pyspark_df from MariaDB
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

print("\nData loaded from workers_pyspark_df:")
df.show(truncate=False)

# Create Spark SQL temporary table
df.createOrReplaceTempView("workers")

# Compute average salary using Spark SQL
result = spark.sql("""
    SELECT
        Gender,
        Department,
        AVG(
            CAST(
                regexp_replace(Salary, ',', '')
                AS DOUBLE
            )
        ) AS Average_Salary
    FROM workers
    GROUP BY Gender, Department
    ORDER BY Gender, Department
""")

print("\nAverage Salary Results - Spark SQL:")
result.show(truncate=False)

spark.stop()
