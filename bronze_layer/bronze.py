import os
import sys

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

class BronzeLayer:
    def __init__(self):
        self.spark = (
            SparkSession.builder
            .appName("BronzeLayer")
            .config(
                "spark.jars.packages",
                "io.delta:delta-spark_2.12:3.3.0,"
                "io.delta:delta-storage:3.3.0,"
            )
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate()
        )

        # Set log level to INFO
        self.spark.sparkContext.setLogLevel("INFO")

        self.table_name = os.environ["TABLE_NAME"]
        self.table_key = os.environ["TABLE_KEY"]
        self.primary_key_fields = self.table_key.split(",")

    def read_delta_table(self):
        df = self.spark.read.format("delta").load(f"../out/delta/{self.table_name}/")
        return df

    def deduplicate_data(self, df):
        window_spec = Window.partitionBy(*self.primary_key_fields).orderBy(F.col("ts_ms").desc())
        df_deduplicated = (df
            .withColumn("row_number", F.row_number().over(window_spec))
            .filter(F.col("row_number") == 1)
            .drop("row_number"))
        return df_deduplicated

    def write_to_parquet(self, df):
        (df
         .write
         .mode("overwrite")
         .option('checkpointLocation', f'../out/bronze/checkpoint_{self.table_name}')
         # .partitionBy(*self.primary_key_fields)
         .parquet(f"../out/bronze/{self.table_name}/")
        )

    def process_delta(self, df):
        # Separate into df_after and df_before
        df_after = df.select("after.*", "op", "ts_ms").filter(F.col("after").isNotNull())
        # Get the columns from the before field if the after field is null and before field is not null
        df_before = df.select("before.*", "op", "ts_ms").filter(F.col("after").isNull() & F.col("before").isNotNull())

        # Union the two DataFrames
        df_union = df_after.union(df_before)

        return df_union

    def backup_and_union_bronze(self, df):
        bronze_path = f"../out/bronze/{self.table_name}/"
        backup_path = f"../out/bronze/backup/{self.table_name}/{datetime.now().strftime('%Y-%m-%d')}/"

        try:
            df_existing = self.spark.read.parquet(bronze_path)
            print("Existing bronze file read successfully.")

            # Save the existing DataFrame to a backup path
            df_existing.write.mode("overwrite").parquet(backup_path)
            print("Existing bronze file backed up successfully.")

            # Union the existing DataFrame with the new DataFrame
            df_combined = df_existing.union(df)
        except Exception as e:
            print(f"Failed to read existing bronze file: {e}")
            df_combined = df

        # Deduplicate the combined DataFrame
        df_combined = self.deduplicate_data(df_combined)

        # Filter out the rows with op as d (delete)
        df_combined = df_combined.filter(F.col("op") != "d")

        return df_combined

    def run(self):
        df = self.read_delta_table()
        processed_delta = self.process_delta(df)
        bronze_df = self.backup_and_union_bronze(processed_delta)
        self.write_to_parquet(bronze_df)

if __name__ == "__main__":
    if os.name == 'nt':
        os.environ['PYSPARK_PYTHON'] = sys.executable
        os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
        os.environ['PATH'] = os.environ['HADOOP_HOME'] + "/bin" + os.pathsep + os.environ['PATH']

    bronze_layer = BronzeLayer()
    bronze_layer.run()