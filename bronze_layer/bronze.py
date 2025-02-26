import os
import sys
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
                "io.delta:delta-core_2.12:1.0.0"
            )
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate()
        )

        self.table_name = os.environ["TABLE_NAME"]
        self.table_key = os.environ["TABLE_KEY"]

    def read_delta_table(self):
        df = self.spark.read.format("delta").load(f"../out/{self.table_name}/delta/")
        return df

    def process_data(self, df):
        # Separate into df_after and df_before
        df_after = df.select("op", "ts_ms", "after.*")
        df_before = df.select("op", "ts_ms", "before.*")

        # Union the two DataFrames
        df_union = df_after.union(df_before)

        # Deduplicate using Window functions
        window_spec = Window.partitionBy("order_id").orderBy(F.desc("ts_ms"))
        df_deduplicated = df_union.withColumn("rank", F.row_number().over(window_spec)).filter(F.col("rank") == 1).drop("rank")

        # Save checkpoint
        checkpoint_path = f"../out/{self.table_name}/bronze/checkpoint"
        df_deduplicated.write.mode("overwrite").option("checkpointLocation", checkpoint_path).parquet(f"../out/{self.table_name}/bronze/")

        return df_deduplicated

    def write_to_parquet(self, df):
        checkpoint_path = f"../out/{self.table_name}/bronze/checkpoint"
        df.write.mode("overwrite").option("checkpointLocation", checkpoint_path).parquet(f"../out/{self.table_name}/bronze/")

    def backup_and_union_bronze(self, df_new):
        bronze_path = f"../out/{self.table_name}/bronze/"
        backup_path = f"../out/{self.table_name}/bronze/backup/"

        try:
            df_existing = self.spark.read.parquet(bronze_path)
            print("Existing bronze file read successfully.")

            # Save the existing DataFrame to a backup path
            df_existing.write.mode("overwrite").parquet(backup_path)
            print("Existing bronze file backed up successfully.")

            # Union the existing DataFrame with the new DataFrame
            df_combined = df_existing.union(df_new)
        except Exception as e:
            print(f"Failed to read existing bronze file: {e}")
            df_combined = df_new

        return df_combined

    def run(self):
        df = self.read_delta_table()
        df_processed = self.process_data(df)
        df_combined = self.backup_and_union_bronze(df_processed)
        self.write_to_parquet(df_combined)

if __name__ == "__main__":
    if os.name == 'nt':
        os.environ['PYSPARK_PYTHON'] = sys.executable
        os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
        os.environ['PATH'] = os.environ['HADOOP_HOME'] + "/bin" + os.pathsep + os.environ['PATH']

    bronze_layer = BronzeLayer()
    bronze_layer.run()