from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
    .appName("Workers RDD Average Salary") \
    .getOrCreate()

sc = spark.sparkContext

# Load workers.txt into an RDD
RDD1 = sc.textFile(
    "/home/pcherukuri3/Assignment3/workers.txt"
)

# Split tab-separated fields
RDD2 = RDD1.map(
    lambda line: line.split("\t")
)

# Create key/value pairs
# Key = (Gender, Department)
# Value = (Salary, Count)
RDD3 = RDD2.map(
    lambda worker: (
        (worker[2], worker[1]),
        (int(worker[3].replace(",", "")), 1)
    )
)

# Sum salary and worker count for each category
RDD4 = RDD3.reduceByKey(
    lambda x, y: (
        x[0] + y[0],
        x[1] + y[1]
    )
)

# Calculate average salary
RDD5 = RDD4.map(
    lambda item: (
        item[0][0],
        item[0][1],
        item[1][0] / item[1][1]
    )
)

# Display results
results = RDD5.collect()

print("\nAverage Salary Results - RDD")
print("--------------------------------")

for gender, department, average in sorted(results):
    print(
        f"{gender:6} {department:6} "
        f"Average Salary = {average:.2f}"
    )

spark.stop()
