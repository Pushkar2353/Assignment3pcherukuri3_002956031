from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.appName("Workers PySpark DataFrame").getOrCreate()

# Load workers.json into a DataFrame
df = spark.read.json("/home/pcherukuri3/Assignment3/workers.json")

# Display the DataFrame
df.show()

# Display the schema
df.printSchema()
