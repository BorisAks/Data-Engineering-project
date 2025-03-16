from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import expr
from pyspark.sql.functions import from_json
import json
import os
import time
os.environ['HADOOP_HOME'] = 'C:/hadoop/hadoop-3.3.6'
os.environ["PATH"] += os.pathsep + os.path.join(os.environ["HADOOP_HOME"], "bin")
# Step 1: Define Spark Session
spark = SparkSession.builder \
    .appName("KafkaToParquet").master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4") \
    .getOrCreate()

KAFKA_TOPIC = "Electric_Vehicle"
# Step 2: Define Schema
schema = StructType([
    StructField('VIN (1-10)', StringType(), True),
    StructField('County', StringType(), True),
    StructField('City', StringType(), True),
    StructField('State', StringType(), True),
    StructField('Postal Code', FloatType(), True),
    StructField('Model Year', IntegerType(), True),
    StructField('Make', StringType(), True),
    StructField('Model', StringType(), True),
    StructField('Electric Vehicle Type', StringType(), True),
    StructField('Clean Alternative Fuel Vehicle (CAFV) Eligibility', StringType(), True),
    StructField('Electric Range', IntegerType(), True),
    StructField('Base MSRP', IntegerType(), True),
    StructField('Legislative District', FloatType(), True),
    StructField('DOL Vehicle ID', IntegerType(), True),
    StructField('Vehicle Location', StringType(), True),
    StructField('Electric Utility', StringType(), True),
    StructField('2020 Census Tract', FloatType(), True)
])

# Step 3: Read data from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

kafka_df.printSchema()

# Step 4: Convert the Kafka value (in binary format) into a string and parse the JSON if needed
parsed_df = kafka_df.selectExpr("CAST(value AS STRING)")
query_console = parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Step 5: Parse the JSON data into the proper schema
final_df = parsed_df.select(from_json("value", schema).alias("data")).select("data.*")

# Step 6: Write to Parquet format
output_path = "C:/Users/boris/Documents/DataEngenier/Spark/final_project/output/raw-data"

query_parquet = final_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("checkpointLocation", "C:/Users/boris/Documents/DataEngenier/Spark/final_project/checkpoints") \
    .option("path", output_path) \
    .start() 

# Stop both queries after they done processing
query_console.awaitTermination()
query_parquet.awaitTermination()

