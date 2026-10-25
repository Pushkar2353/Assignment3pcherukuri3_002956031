from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.appName("Workers PySpark DataFrame").getOrCreate()

# Load workers.json into a DataFrame
df = spark.read.option("multiLine", "true").json(
    "/home/pcherukuri3/Assignment3/workers.json"
)

# Display the DataFrame
df.show(truncate=False)

# Display the schema
df.printSchema()

# Save DataFrame to MariaDB
df.write \
    .format("jdbc") \
    .option("url", "jdbc:mariadb://192.168.1.5:3306/GSU") \
    .option("dbtable", "workers_pyspark_df") \
    .option("user", "dbadmin") \
    .option("password", "testpassword") \
    .option("driver", "org.mariadb.jdbc.Driver") \
    .mode("overwrite") \
    .save()

spark.stop()
