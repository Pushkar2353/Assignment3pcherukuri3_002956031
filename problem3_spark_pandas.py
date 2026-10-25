from pyspark.sql import SparkSession

# Create SparkSession
# Disable ANSI mode for compatibility with Pandas API on Spark
spark = SparkSession.builder \
    .appName("Workers Spark Pandas Average Salary") \
    .config("spark.sql.ansi.enabled", "false") \
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

print("\nData loaded from workers_pyspark_df:")
df.show(truncate=False)

# Convert Spark DataFrame to Pandas API on Spark DataFrame
psdf = df.pandas_api()

# Convert Salary from text such as "120,000" to numeric
psdf["SalaryNumeric"] = (
    psdf["Salary"]
    .str.replace(",", "", regex=False)
    .astype(float)
)

# Compute average salary using Pandas API on Spark
result = (
    psdf
    .groupby(["Gender", "Department"])["SalaryNumeric"]
    .mean()
    .reset_index()
)

result = result.rename(
    columns={"SalaryNumeric": "Average_Salary"}
)

print("\nAverage Salary Results - Spark Pandas:")
print(result)

spark.stop()
