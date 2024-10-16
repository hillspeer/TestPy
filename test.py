from pyspark.sql import SparkSession
from pyspark.sql import Row
import time

spark = SparkSession.builder \
    .appName("Postgres Insert Example") \
    .config("spark.jars", "/opt/tmp/postgresql-42.2.5.jar") \
    .config("spark.executor.memory", "1g") \
    .config("spark.executor.cores", "1") \
    .config("spark.cores.max", "1") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .config("fs.defaultFS", "file:///") \
    .config("spark.local.dir","/opt/tmp") \
    .master("spark://localhost:7077") \
    .getOrCreate()

print(spark.conf.get("fs.defaultFS"))

userDf = spark.read.csv("/tmp/test.csv", header=True, inferSchema=True)

#csvdf.write.mode("overwrite").parquet("/opt/tmp/test.parquet")

#userDf = spark.read.parquet('/tmp/test.parquet')

userDf.printSchema()

# Define connection properties
jdbc_url = "jdbc:postgresql://localhost:5432/hellodb"
connection_properties = {
    "user": "kaderhillsonpeer",
    "password": "xxxxxx",
    "driver": "org.postgresql.Driver"
}

# Write DataFrame to PostgreSQL table
userDf.write \
    .mode("overwrite") \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "library.user") \
    .options(**connection_properties) \
    .save()

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("Stopping Spark job...")
finally:
    spark.stop()
