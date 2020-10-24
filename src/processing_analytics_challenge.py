## import libraries
# import sys
# sys.path.append("../") # go to parent dir

# data handler
from src.data_handler import DataHandler
from src.utils.dependencies import *


class ProcessingAnalyticsChallenge:

    @staticmethod
    def create_spark_session():
        # create or get spark session locally
        spark = SparkSession.builder \
            .master("local[*]") \
            .appName("PayPayChallenge") \
            .getOrCreate()
        return spark

    @staticmethod
    def duration(start, end):
        """
        Calculate timestamp interval with previous request
        """
        try:
            num_of_seconds = (end - start).total_seconds()
        except:
            num_of_seconds = 0
        return num_of_seconds

    @staticmethod
    def sessionize(df_logs, gd):
        """
        Aggregate the page hits by session
        Session format is
        ip_session_info : <IP_address>_<session_count>
        count_session: session count
        """

        logger.info("1. Sessionize the web log by IP = aggregate all page hits by visitor/IP during a session")

        window_func_ip = Window.partitionBy(
            "client_ip").orderBy("current_timestamp")
        df = df_logs.withColumn("previous_timestamp",
                                lag(col("current_timestamp")).over(window_func_ip)) \
            .withColumn("session_duration",
                        gd(col("previous_timestamp"), col("current_timestamp"))) \
            .withColumn("is_new_session",
                        when((col("session_duration") > session_time), 1).otherwise(0)) \
            .withColumn("count_session",
                        sum(col("is_new_session")).over(window_func_ip)) \
            .withColumn("ip_session_info",
                        concat_ws("_", col("client_ip"), col("count_session")))
        sessionized_df = df.select(["ip_session_info", "client_ip", "request_url",
                                    "previous_timestamp", "current_timestamp",
                                    "session_duration", "is_new_session", "count_session"]).orderBy("count_session",
                                                                                                    ascending=False)
        sessionized_df[["ip_session_info", 'count_session']].distinct().orderBy("count_session", ascending=False).show()

        return sessionized_df

    @staticmethod
    def average_session_time(df_ip_session):
        """
        Calculate average session time
        Calculate total duration for all session, and get average value of this.
        """

        logger.info("2. Determine the average session time")

        window_func_session = Window.partitionBy("ip_session_info").orderBy("current_timestamp")
        df_session = df_ip_session.withColumn("previous_timestamp_session",
                                              lag(df_ip_session["current_timestamp"]).over(window_func_session)) \
            .withColumn("current_session_duration",
                        get_duration(col("previous_timestamp_session"), col("current_timestamp")))
        df_session_total = df_session.groupby("ip_session_info").agg(
            sum("current_session_duration").alias("total_session_time")).cache()
        df_session_avg = df_session_total.select([mean("total_session_time").alias("avg_session_time")]).cache()
        df_session_avg.show()
        return df_session

    @staticmethod
    def count_unique_request(df_session):
        """
        Get number of unique url per session
        Group by session_info, and count all unique urls
        """

        logger.info(
            "3. Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session")

        df_unique_url = df_session.groupby("ip_session_info").agg(
            countDistinct("request_url").alias("count_unique_requests"))
        df_unique_url.show()
        return df_unique_url

    @staticmethod
    def get_longest_session_time(df_session):
        """
        Sort user with longest session times
        Group by session, and count all unique urls
        """

        logger.info("4. Find the most engaged users, ie the IPs with the longest session times")

        df_ip_time = df_session.groupby("client_ip").agg(
            sum("session_duration").alias("total_session_time"),
            count("client_ip").alias("num_sessions"),
            max("session_duration").alias("session_duration_max")) \
            .withColumn("avg_session_time", col("total_session_time") / col("num_sessions")) \
            .orderBy(col("avg_session_time"), ascending=False)
        df_ip_time[["client_ip", "total_session_time"]].orderBy("total_session_time", ascending=False).show()
        return df_ip_time


if __name__ == "__main__":
    pac = ProcessingAnalyticsChallenge()
    dh = DataHandler()
    spark = pac.create_spark_session()
    df_logs = dh.initialization(spark=spark).cache()

    get_duration = udf(pac.duration, FloatType())

    # print(df_logs.limit(5).toPandas().head(5))
    df_ip_session = pac.sessionize(df_logs, gd=get_duration).repartition(num_partitions).cache()
    df_session = pac.average_session_time(df_ip_session).repartition(num_partitions).cache()
    df_unique_url = pac.count_unique_request(df_session)
    df_ip_time = pac.get_longest_session_time(df_session)
