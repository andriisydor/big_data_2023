from pyspark.sql import Window
from pyspark.sql.functions import col, date_format, desc, max


class QueryManager:
    def __init__(self, spark, trip_fare_df, trip_data_df):
        self.spark = spark
        self.trip_fare_df = trip_fare_df
        self.trip_data_df = trip_data_df

    @staticmethod
    def trips_count(dataframe, date_column):
        """
        Args:
            dataframe: pyspark.sql.DataFrame
            date_column: desired date column in dataframe
        Returns:
            dataframe which has three columns
            1. Vendor_ID
            2. Day of Week
            3. Count (count of trips)
        """
        trip_df = dataframe.withColumn("dayofweek",
                                       date_format(dataframe[date_column], "EEEE"))
        trips_by_week = (trip_df.filter(col("vendor_id") != "None").groupBy("vendor_id", "dayofweek").
                         count().orderBy(desc("vendor_id"), desc("count")).withColumn("max_trip_count",
                                                                                      max("count").over(
                                                                                          Window.partitionBy(
                                                                                              "vendor_id")))
                         .filter(col("count") == col("max_trip_count")).drop("max_trip_count"))
        return trips_by_week
