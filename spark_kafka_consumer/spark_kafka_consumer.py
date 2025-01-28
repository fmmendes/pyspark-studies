import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, schema_of_json

print(f'Environment variables:')
print(f'JAVA_HOME: {os.environ["JAVA_HOME"]}')
print(f'HADOOP_HOME: {os.environ["HADOOP_HOME"]}')
print(f'Path: {os.environ["Path"]}')

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SparkKafkaConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2") \
    .getOrCreate()

try:
    # Define Kafka parameters
    kafka_bootstrap_servers = os.environ["BOOTSTRAP_SERVERS"]
    kafka_topic = os.environ["TOPIC"]

    # Read from Kafka
    df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .load()

    # Verify if the df is empty, if it is, raise an exception
    if df.count() == 0:
        df.show()
        raise ValueError("No data in the Kafka topic")

    # Convert the value column from binary to string
    df = df.selectExpr("CAST(value AS STRING)")

    # Infer the schema from a sample JSON message
    sample_json = df.select("value").first()[0]
    schema = schema_of_json(sample_json)

    # Parse the JSON messages using the inferred schema
    df = df.withColumn("value", from_json(col("value"), schema))

    # Select the fields you are interested in
    df = df.select(col("value.after.*"))

    df.show()

    # save the data to a parquet file
    df.write.mode("append").parquet("output")

except ValueError as e:
    print(f"An ValueError occurred: {e}")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    # Stop any active streaming queries
    for query in spark.streams.active:
        query.stop()

    # Clear any cached data
    spark.catalog.clearCache()
    # Stop the Spark session
    spark.stop()