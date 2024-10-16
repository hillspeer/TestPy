from pyspark.sql import SparkSession
from pyspark.sql import Row
import time
# Create Spark session
spark = SparkSession.builder \
    .appName("Postgres Insert Example") \
    .config("spark.jars", "/opt/tmp/postgresql-42.2.5.jar") \
    .config("spark.executor.memory", "1g") \
    .config("spark.executor.cores", "2") \
    .config("spark.cores.max", "1") \
    .master("spark://localhost:7077") \
    .getOrCreate()

# Create DataFrame to insert

SOME_ENVIRONMENT = 1#spark.conf.get("test")

for i in range(3):
    userDf = spark.createDataFrame([
        Row(id=1, email='abdul@gmail.com', full_name="Abdul hameed"+str(SOME_ENVIRONMENT), password="abdul@123"),
        Row(id=2, email='Vijay@gmail.com', full_name="Vijay Kumar"+str(SOME_ENVIRONMENT), password="vijay@123"),
        Row(id=3, email='Test@gmail.com', full_name="Vijay Kumar"+str(SOME_ENVIRONMENT), password="test@123"),
        Row(id=4, email='Ajay@gmail.com', full_name="Abdul hameed"+str(SOME_ENVIRONMENT), password="abdul@123"),
        Row(id=5, email='Vinay@gmail.com', full_name="Vinay Kumar"+str(SOME_ENVIRONMENT), password="vijay@123"),
        Row(id=6, email='check@gmail.com', full_name="Checking"+str(SOME_ENVIRONMENT), password="test@123")
        ])
    time.sleep(3)

userDf.printSchema()

# Define connection properties
jdbc_url = "jdbc:postgresql://localhost:5432/hellodb"
connection_properties = {
    "user": "kaderhillsonpeer",
    "password": "lifcom84",
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