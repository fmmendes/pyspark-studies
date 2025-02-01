import os
import sys
from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql import SparkSession


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
            .config("spark.sql.debug.maxToStringFields", "1000")  # Adjust the value as needed
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
            temp_df = temp_df.withColumn('schema', F.schema_of_json('{}'))
            print("No data found in the Kafka topic.")
        else:
            temp_df = temp_df.withColumn("schema", F.schema_of_json(df_json.select("value").head()[0]))

        schema = temp_df.select('schema').collect()[0][0]

        print(f'Inferred schema: {str(schema)}')

        before = "before: STRUCT"
        before_string = "before: STRING"
        after = "after: STRUCT"
        after_string = "after: STRING"
        op_string = "op: STRING"

        if before_string in schema and schema.find(after) > -1:
            schema = schema.replace(
                before_string, schema[schema.find(after):schema.find(before_string) - 2].replace(after, before)
            )
        elif after_string in schema and schema.find(before) > -1:
            schema = schema.replace(
                after_string, schema[schema.find(before):schema.find(op_string) - 2].replace(before, after)
            )

        schema = (
            schema.replace("BIGINT", "STRING")
            .replace("BOOLEAN", "STRING")
            .replace('BINARY', "STRING")
            .replace('INT', "STRING")
            .replace('TIMESTAMP', "STRING")
        )

        print(f'Final schema: {str(schema)}')

        return schema

    def process_data(self, df, schema):
        df = df.selectExpr("CAST(value AS STRING) as value")
        df = df.withColumn("value", F.from_json(F.col("value"), schema))
        df = df.select(F.col("value.after.*"))
        return df

    def save_to_delta(self, df, schema):
        topic_empty_value = 'false' if schema != 'STRUCT<>' else 'true'

        writer = (
            df
            .withColumn("value", F.from_json("value", schema))
            .withColumn("date", F.date_format(F.current_timestamp(), 'yyyy-MM-dd'))
            .withColumn("topicEmpty", F.lit(topic_empty_value))
        )

        writer.filter(writer.value.isNotNull()) \
            .withColumn("value", F.from_json(F.col("value"), schema)) \
            .select(F.col('value.*'), F.col('date'), F.col('topicEmpty')) \
            .write \
            .format('delta') \
            .partitionBy('date') \
            .mode('append') \
            .option("mergeSchema", "true") \
            .option('checkpointLocation', f'../out/checkpoint_{self.topic}') \
            .save(f'../out/{self.topic}')

    def run(self):
        self.print_spark_parameters()
        self.print_kafka_parameters()

        # Read data from Kafka
        df = self.read_from_kafka()
        # Infer schema from the data
        schema = self.infer_schema(df)

        df = self.process_data(df, schema)
        self.save_to_delta(df, schema)


if __name__ == "__main__":
    service = DeltaService()
    service.run()
