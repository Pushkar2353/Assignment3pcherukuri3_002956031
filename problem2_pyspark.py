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

spark.stop()
