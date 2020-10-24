from pyspark.sql.types import StructField, StructType, StringType, IntegerType

"""
schema based on AWS Elastic Load Balancer 
https://docs.aws.amazon.com/athena/latest/ug/elasticloadbalancer-classic-logs.html

"""

schema = StructType([
    StructField("timestamp", StringType(), False),
    StructField("elb", StringType(), False),
    StructField("client:port", StringType(), False),
    StructField("backend:port", StringType(), False),
    StructField("request_processing_time", StringType(), False),
    StructField("backend_processing_time", StringType(), False),
    StructField("response_processing_time", StringType(), False),
    StructField("elb_status_code", IntegerType(), False),
    StructField("backend_status_code", StringType(), False),
    StructField("received_bytes", StringType(), False),
    StructField("sent_bytes", StringType(), False),
    StructField("request", StringType(), False),
    StructField("user_agent", StringType(), False),
    StructField("ssl_cipher", StringType(), False),
    StructField("ssl_protocol", StringType(), False)
])
