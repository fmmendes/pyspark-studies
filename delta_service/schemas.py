import pyspark.sql.types as T
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

schema = T.StructType([
    T.StructField("after", T.StructType([
        T.StructField("customer_id", T.IntegerType(), True),
        T.StructField("order_id", T.IntegerType(), True),
        T.StructField("price", T.IntegerType(), True),
        T.StructField("product_id", T.IntegerType(), True),
        T.StructField("quantity", T.IntegerType(), True)
    ]), True),
    T.StructField("before", T.StructType([
        T.StructField("customer_id", T.IntegerType(), True),
        T.StructField("order_id", T.IntegerType(), True),
        T.StructField("price", T.IntegerType(), True),
        T.StructField("product_id", T.IntegerType(), True),
        T.StructField("quantity", T.IntegerType(), True)
    ]), True),
    T.StructField("op", T.StringType(), True),
    T.StructField("source", T.StructType([
        T.StructField("connector", T.StringType(), True),
        T.StructField("db", T.StringType(), True),
        T.StructField("file", T.StringType(), True),
        T.StructField("gtid", T.StringType(), True),
        T.StructField("name", T.StringType(), True),
        T.StructField("pos", T.StringType(), True),
        T.StructField("query", T.StringType(), True),
        T.StructField("row", T.StringType(), True),
        T.StructField("sequence", T.StringType(), True),
        T.StructField("server_id", T.IntegerType(), True),
        T.StructField("snapshot", T.StringType(), True),
        T.StructField("table", T.StringType(), True),
        T.StructField("thread", T.StringType(), True),
        T.StructField("ts_ms", T.DoubleType(), True),
        T.StructField("version", T.StringType(), True)
    ]), True),
    T.StructField("transaction", StringType(), True),
    T.StructField("ts_ms", DoubleType(), True)
])