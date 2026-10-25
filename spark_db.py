from pyspark.sql import SparkSession
# Create SparkSession
spark = SparkSession.builder.appName("Reading DB table")\
             .getOrCreate()
# 5.Reading a JDBC table with custom query:
df = spark.read.format("jdbc")\
          .option("url", "jdbc:mysql://192.168.1.6:3306/GSU?permitMysqlScheme")\
          .option("dbtable", "CSstudents")\
          .option("user", "dbadmin")\
          .option("password", "testpassword")\
          .option("driver", "org.mariadb.jdbc.Driver")\
          .load()
df.show()
