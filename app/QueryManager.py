from pyspark.sql import Window
from pyspark.sql.functions import col, date_format, desc, max
import pyspark.sql.functions as f
from app import columns


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

    def trips_with_tip_mount_greater_than_fare_amount(self):
        """ Data of trips with tips amount greater than the fare amount.

        Returns:
            dataframe with columns:
            medallion, hack_license, vendor_id, pickup_datetime, payment_type, fare_amount, tip_amount.
        """

        result_columns_names = [columns.medallion, columns.hack_license, columns.vendor_id, columns.pickup_datetime,
                                columns.payment_type, columns.fare_amount, columns.tip_amount]
        trips_with_tip_mount_greater_than_fare_amount = (
            self.trip_fare_df.filter(col(columns.fare_amount) < col(columns.tip_amount)).select(*result_columns_names)
        )
        return trips_with_tip_mount_greater_than_fare_amount

    def total_earnings_of_each_vendor_for_first_seven_days_of_january(self):
        """ Sum of earning of each vendor for trips that started on each of the first seven days of January 2013.

        Returns:
            dataframe with columns:
            vendor_id, date(in format yyyy-MM-dd), total_earnings.
        """
        column_date = 'date'
        column_total_earnings = 'total_earnings'
        start_date_string = '2012-12-31 23:59:59.59'
        end_date_string = '2013-01-07 23:59:59.59'

        total_earnings_of_each_vendor_for_first_seven_days_of_january = (
            self.trip_fare_df
                .withColumn(column_date, date_format(self.trip_fare_df[columns.pickup_datetime], 'yyyy-MM-dd'))
                .filter(col(column_date).between(start_date_string, end_date_string))
                .orderBy(columns.vendor_id, column_date)
                .groupBy(columns.vendor_id, column_date)
                .agg(f.sum(columns.total_amount).alias(column_total_earnings))
        )
        return total_earnings_of_each_vendor_for_first_seven_days_of_january

    def driver_of_each_day(self):
        """
        """
        column_date = 'date'
        column_tips_sum = 'tips_sum'
        column_max_tips_sum = 'max_tips_sum'
        join_column_names = [columns.vendor_id, columns.medallion, columns.hack_license, columns.pickup_datetime]
        joined_df = self.trip_fare_df.join(self.trip_data_df, join_column_names, 'left')
        drivers = (
            joined_df
                .withColumn('date', f.date_format(joined_df[columns.dropoff_datetime], 'yyyy-MM-dd'))
                .groupBy(columns.vendor_id, columns.hack_license, column_date)
                .agg(f.sum(columns.tip_amount).alias(column_tips_sum))
                .orderBy(column_date, f.desc(column_tips_sum))
                .withColumn(column_max_tips_sum, f.max(f.col(column_tips_sum))
                            .over(Window.partitionBy(column_date)).alias(column_max_tips_sum))
                .filter(f.col(column_max_tips_sum) == f.col(column_tips_sum))
                .select(column_date, columns.hack_license, columns.vendor_id, column_tips_sum)
        )
        return drivers
