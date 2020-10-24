from src.log_file_schema import schema
from src.utils.dependencies import *


class DataHandler(object):
    """
    class to load and preprocess log data
    """
    @staticmethod
    def initialization(spark):
        df = spark.read.csv(log_file, schema=schema, sep=" ").repartition(
            num_partitions).cache()
        split_client = split(df["client:port"], ":")
        split_backend = split(df["backend:port"], ":")
        split_request = split(df["request"], " ")

        return df.where(col("timestamp").isNotNull()
                        & col("client:port").isNotNull()
                        & col("request").isNotNull()) \
            .withColumn("client_ip", split_client.getItem(0)) \
            .withColumn("client_port", split_client.getItem(1)) \
            .withColumn("backend_ip", split_backend.getItem(0)) \
            .withColumn("backend_port", split_backend.getItem(1)) \
            .withColumn("request_action", split_request.getItem(0)) \
            .withColumn("request_url", split_request.getItem(1)) \
            .withColumn("request_protocol", split_request.getItem(2)) \
            .withColumn("current_timestamp", col("timestamp").cast("timestamp")) \
            .drop("client:port", "backend:port", "request") \
            .filter(col("elb_status_code") < 400).cache()
