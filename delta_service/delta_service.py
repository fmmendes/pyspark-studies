import os
import sys
import time

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from schemas import schema  # Import the schema

class DeltaService:
    def __init__(self):
        self.spark = (
            SparkSession.builder
            .appName("DeltaService")
            .config(
                "spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2,"
                "io.delta:delta-spark_2.12:3.3.0,"
                "io.delta:delta-storage:3.3.0,"
                "org.antlr:antlr4-runtime:4.13.2"
            )
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.debug.maxToStringFields", "1000")  # Adjust the value as needed
            .getOrCreate()
        )

        # Set log level to INFO
        self.spark.sparkContext.setLogLevel("INFO")

        self.bootstrap_servers = os.environ["BOOTSTRAP_SERVERS"]
        self.table_name = os.environ["TABLE_NAME"]
        self.table_key = os.environ["TABLE_KEY"]
        self.topic = os.environ["TOPIC"]

    def print_spark_parameters(self):
        print("Spark parameters:")
        print(f'Apache Spark version: {self.spark.version}')
        print(f'Apache Spark Context version: {self.spark.sparkContext.version}')
        print(f'Apache Spark application name: {self.spark.sparkContext.appName}')
        print(f'Apache Spark Configurations: {self.spark.sparkContext.getConf().getAll()}')

    def print_kafka_parameters(self):
        print('Kafka parameters:')
        print(f'Using Kafka bootstrap servers: {self.bootstrap_servers}')
        print(f'Using Kafka topic: {self.topic}')

    def read_kafka_stream(self):
        df = (
            self.spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.bootstrap_servers)
            .option("subscribe", self.topic)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
        )
        return df

    def process_data(self, df):
        df = (
            df
            .selectExpr("CAST(value AS STRING) as value")
            .withColumn("value", F.from_json("value", schema))
            .withColumn("date", F.date_format(F.current_timestamp(), 'yyyy-MM-dd'))
        )
        return df

    def save_to_delta(self, df):
        query = (
            df
            .select(F.col("value.*"), F.col("date"))
            .writeStream
            .format('delta')
            .partitionBy('date')
            .outputMode('append')
            .option("mergeSchema", "true")
            .option('checkpointLocation', f'../out/{self.table_name}/delta/checkpoint')
            .start(f'../out/{self.table_name}/delta/')
        )

        query.awaitTermination()

    def run(self):
        self.print_spark_parameters()
        self.print_kafka_parameters()

        # Read data from Kafka
        df = self.read_kafka_stream()

        df = self.process_data(df)
        self.save_to_delta(df)


if __name__ == "__main__":
    if os.name == 'nt':
        os.environ['PYSPARK_PYTHON'] = sys.executable
        os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
        os.environ['PATH'] = os.environ['HADOOP_HOME'] + "/bin" + os.pathsep + os.environ['PATH']

    service = DeltaService()
    service.run()
