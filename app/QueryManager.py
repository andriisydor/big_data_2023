from pyspark.sql import Window
import pyspark.sql.functions as f
from app import columns


class QueryManager:
    def __init__(self, spark, trip_fare_df, trip_data_df):
        self.spark = spark
        self.trip_fare_df = trip_fare_df
        self.trip_data_df = trip_data_df

    def trips_count(self, date_column):
        """
        Args:
            date_column: desired date column in dataframe
        Returns:
            dataframe which has three columns
            1. Vendor_ID
            2. Day of Week
            3. Count (count of trips)
        """
        trip_df = self.trip_data_df.withColumn('dayofweek',
                                               f.date_format(self.trip_data_df[date_column], 'EEEE'))
        trips_by_week = (trip_df.filter(f.col(columns.vendor_id) != 'None').groupBy(columns.vendor_id, 'dayofweek').
                         count().orderBy(f.desc(columns.vendor_id), f.desc('count')).withColumn('max_trip_count',
                                                                                            f.max('count').over(
                                                                                                Window.partitionBy(
                                                                                                    'vendor_id')))
                         .filter(f.col('count') == f.col('max_trip_count')).drop('max_trip_count'))
        return trips_by_week

    def total_revenue(self):
        """ Calculates the total revenue of each vendor
             Returns:
            DataFrame: A DataFrame containing the total revenue for each vendor.
        """
        dataframe = (self.trip_fare_df.filter(f.col(columns.vendor_id) != 'None').groupBy(columns.vendor_id)
                     .agg(f.format_number(f.sum(columns.total_amount), 2).alias('total revenue')))
        return dataframe

    def avg_trip_distance(self):
        """
            Calculates the average trip distance for different numbers of passengers.

            Returns:
                DataFrame: A DataFrame containing the average trip distance for each combination of vendor
                and passenger count.
        """
        dataframe = (self.trip_data_df.filter(f.col(columns.passenger_count).
                                              isNotNull()).groupBy(columns.vendor_id, columns.passenger_count).
                     agg(f.avg(columns.trip_distance)).orderBy(f.desc(columns.passenger_count)))
        return dataframe

    def simultaneous_trips(self):
        """
        Calculates the maximum number of simultaneous trips that happened on the same day.

        Returns:
            DataFrame: A DataFrame containing the maximum number of simultaneous trips for the top 5 days.
        """
        pickup_dataframe = (self.trip_data_df.filter(f.col(columns.pickup_datetime).isNotNull()).
                            select(f.col(columns.pickup_datetime).alias('event_time'),
                                   f.lit(1).alias('event_count')))
        dropoff_dateframe = (self.trip_data_df.filter(f.col(columns.dropoff_datetime).isNotNull()).
                             select(f.col(columns.dropoff_datetime).alias('event_time'),
                                    f.lit(-1).alias('event_count')))
        event_dateframe = pickup_dataframe.union(dropoff_dateframe)
        dataframe = event_dateframe.withColumn('sum', f.sum('event_count').over(Window.partitionBy('event_time')
                                                                              .orderBy(f.asc('event_time'))))
        dataframe = dataframe.groupBy(f.date_format('event_time', 'yyyy-MM-dd').alias('day')
                                      ).agg(f.max('sum').alias('simultaneous_trips')).orderBy(
            f.desc(f.col('simultaneous_trips'))).limit(5)
        return dataframe

    def most_expensive_trips(self):
        """
        Calculates the most expensive trips for each vendor.

        Returns:
            DataFrame: A DataFrame containing the most expensive trips for each vendor.
        """
        dataframe = self.trip_fare_df.groupBy(columns.vendor_id).agg(f.max(columns.total_amount).
                                                                     alias(columns.total_amount))
        return dataframe

    def avg_amount_rate_code(self):
        """
        Calculates the count of trips with a tip above the average tip amount for trips with different rate codes.

        Returns:
            DataFrame: A DataFrame containing the count of such trips for each rate code.
        """
        dataframe = self.trip_fare_df.join(self.trip_data_df, ['medallion', 'hack_license', 'vendor_id',
                                                               'pickup_datetime'], 'inner')
        average_tip_amounts = dataframe.groupBy(columns.rate_code).agg(f.avg(columns.tip_amount)
                                                                        .alias('avg_tip_amount'))
        joined_data = dataframe.join(average_tip_amounts, on=columns.rate_code, how='inner')
        dataframe = joined_data.withColumn('tip_above_avg', f.col('tip_amount') > f.col('avg_tip_amount'))
        dataframe = (dataframe.groupBy(columns.rate_code).count().withColumnRenamed('count', 'trip_count').
                     orderBy(f.desc('trip_count')))
        return dataframe

    def trips_with_tip_mount_greater_than_fare_amount(self):
        """ Data of trips with tips amount greater than the fare amount.

        Returns:
            dataframe with columns:
            medallion, hack_license, vendor_id, pickup_datetime, payment_type, fare_amount, tip_amount.
        """

        result_columns_names = [columns.medallion, columns.hack_license, columns.vendor_id, columns.pickup_datetime,
                                columns.payment_type, columns.fare_amount, columns.tip_amount]
        trips_with_tip_mount_greater_than_fare_amount = (
            self.trip_fare_df.filter(f.col(columns.fare_amount) < f.col(columns.tip_amount))
                .select(*result_columns_names)
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
                .withColumn(column_date, f.date_format(self.trip_fare_df[columns.pickup_datetime], 'yyyy-MM-dd'))
                .filter(f.col(column_date).between(start_date_string, end_date_string))
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
        joined_df = self.trip_fare_df.join(self.trip_data_df, join_column_names, 'inner')
        drivers = (
            joined_df.withColumn('date', f.date_format(joined_df[columns.dropoff_datetime], 'yyyy-MM-dd'))
                     .groupBy(columns.vendor_id, columns.hack_license, column_date)
                     .agg(f.sum(columns.tip_amount).alias(column_tips_sum))
                     .orderBy(column_date, f.desc(column_tips_sum))
                     .withColumn(column_max_tips_sum, f.max(f.col(column_tips_sum))
                                 .over(Window.partitionBy(column_date)).alias(column_max_tips_sum))
                     .filter(f.col(column_max_tips_sum) == f.col(column_tips_sum))
                     .select(column_date, columns.hack_license, columns.vendor_id, column_tips_sum)
        )
        return drivers

    def price_per_second_of_drive_for_each_vendor(self):
        column_average_fare_per_second = 'average_fare_per_second'
        join_column_names = [columns.vendor_id, columns.medallion, columns.hack_license, columns.pickup_datetime]
        joined_df = self.trip_fare_df.join(self.trip_data_df, join_column_names, 'inner')
        price_per_second_of_drive_for_each_vendor = (
            joined_df.groupBy('vendor_id')
                     .agg({columns.fare_amount: 'sum', columns.trip_time_in_secs: 'sum'})
                     .withColumn(column_average_fare_per_second,
                                 f.col('sum(fare_amount)') / f.col('sum(trip_time_in_secs)'))
                     .select(columns.vendor_id, column_average_fare_per_second)
        )
        return price_per_second_of_drive_for_each_vendor
