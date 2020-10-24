# common dependencies and default setups
# pyspark session
from pyspark.sql import SparkSession

from pyspark.sql.window import Window
from pyspark.sql.functions import lag, max, sum, mean, col, when, count, countDistinct, split, lit,  concat_ws, udf, to_utc_timestamp, unix_timestamp, from_unixtime
from pyspark.sql.types import FloatType

# logging
from loguru import  logger

import pandas as pd

# set options for display in pandas
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('max_colwidth', 50)


# fixed stuff
log_file = '../data/2015_07_22_mktplace_shop_web_log_sample.log.gz'
num_partitions = 8
session_time = 15 * 60
