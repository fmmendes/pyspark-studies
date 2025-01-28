import os
import sys
from datetime import datetime

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

class DeltaService:
    def __init__(self):
        os.environ['PYSPARK_PYTHON'] = sys.executable
        os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
        os.environ['PATH'] = os.environ['HADOOP_HOME'] + "/bin" + os.pathsep + os.environ['PATH']

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
                .getOrCreate()
        )

        self.bootstrap_servers = os.environ["BOOTSTRAP_SERVERS"]
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

    def read_from_kafka(self):
        df = (
            self.spark
                .read
                .format("kafka")
                .option("kafka.bootstrap.servers", self.bootstrap_servers)
                .option("subscribe", self.topic)
                .load()
        )

        if df.rdd.isEmpty():
            print("No data found in the Kafka topic.")
            sys.exit(1)

        return df

    def infer_schema(self, df):
        df_json = (
            df.withColumn("value", F.expr("string(value)"))
                .filter((F.col("value").isNotNull()) | (F.col("value") != ""))
                .select("key", F.expr("struct(offset, value) r"))
                .groupby("key").agg(F.expr("max(r) r"))
                .select("r.value")
        )

        df_json.show()
        df_json.printSchema()

        temp_df = self.spark.range(1)
        if df_json.rdd.isEmpty():
            print("No data found in the Kafka topic.")
            sys.exit(1)

        temp_df = temp_df.withColumn("schema", F.schema_of_json(df_json.select("value").head()[0]))

        schema = temp_df.select('schema').collect()[0][0]

        return schema

    def process_data(self, df, schema):
        df = df.selectExpr("CAST(value AS STRING)")
        df = df.withColumn("value", F.from_json(F.col("value"), schema))
        df = df.select(F.col("value.after.*"))
        return df

    def save_to_delta(self, df):
        # Write data to Delta Lake appending the datetime to the path
        df.write.format("delta").mode("append").save(f'/bronze/delta/{self.topic}/datetime={datetime.now().strftime("%Y-%m-%d-%H-%M-%S")}')

    def run(self):
        self.print_spark_parameters()
        self.print_kafka_parameters()

        # Read data from Kafka
        df = self.read_from_kafka()
        # Infer schema from the data
        schema = self.infer_schema(df)
        # TODO Process and normalize the Schema


        df = self.process_data(df, schema)
        self.save_to_delta(df)

if __name__ == "__main__":
    service = DeltaService()
    service.run()