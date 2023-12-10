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
            dataframe: pyspark.sql.DataFrame
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
                DataFrame: A DataFrame containing the average trip distance for each combination of vendor and passenger
                count.
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
        average_tip_amounts = dataframe.groupBy(columns.rate_code).agg(f.avg(columns.tip_amount).alias('avg_tip_amount'))
        joined_data = dataframe.join(average_tip_amounts, on=columns.rate_code, how='inner')
        dataframe = joined_data.withColumn('tip_above_avg', f.col('tip_amount') > f.col('avg_tip_amount'))
        dataframe = (dataframe.groupBy(columns.rate_code).count().withColumnRenamed('count', 'trip_count').
                     orderBy(f.desc('trip_count')))
        return dataframe

    def tips_count(self):
        """ Identifies the specific day of the week when each vendor tends to receive the highest amount of tips.

        Returns:
            DataFrame: A DataFrame containing the day of the week and the corresponding highest amount of tips received
            for each vendor.
        """
        window_spec = Window.partitionBy(columns.vendor_id).orderBy(f.col("total_tips").desc())

        dataframe = (self.trip_fare_df.withColumn("day_of_week", f.date_format(columns.pickup_datetime, 'EEEE'))
                         .groupBy(columns.vendor_id, "day_of_week")
                         .agg(f.format_number(f.sum(columns.tip_amount), 2).alias("total_tips"))
                         .withColumn("rank", f.row_number().over(window_spec))
                         .filter(f.col("rank") == 1)
                         .select(columns.vendor_id, "day_of_week", "total_tips"))

        return dataframe

    def avg_fare_amount_payment(self):
        """ Calculates the average fare amount for each payment type.

        Returns:
            DataFrame: A DataFrame containing the average fare amount for each payment type.
        """
        dataframe = (self.trip_fare_df.groupBy(columns.payment_type)
                         .agg(f.format_number(f.avg(columns.fare_amount), 2).alias("average_fare_amount"))
                         .orderBy(f.desc("average_fare_amount")))

        return dataframe

    def top_vendor_drivers(self):
        """ Identifies the top 10 drivers for each vendor based on average trip distance and total tip amount.

        Returns:
            DataFrame: A DataFrame containing the vendor ID, unique driver license, average mileage covered, total tip
            amount received and the corresponding rank.
        """
        joined_df = (self.trip_data_df.withColumnRenamed(columns.vendor_id, "vendor")
                                      .join(self.trip_fare_df, [columns.hack_license, columns.pickup_datetime],
                                            'inner'))

        window_spec = Window.partitionBy("vendor").orderBy(f.desc("average mileage"), f.desc("total tip amount"))
        dataframe = (joined_df.groupBy(["vendor", columns.hack_license])
                              .agg(f.format_number(f.avg(columns.trip_distance), 2).alias('average mileage'),
                                   f.format_number(f.sum(columns.tip_amount), 2).alias('total tip amount'))
                              .withColumn("rank", f.rank().over(window_spec))
                              .filter(f.col("rank") <= 10))

        return dataframe

    def percentage_long_trips(self):
        """ Calculates the percentage of trips with a duration greater than 30 minutes for each vendor.

        Returns:
            DataFrame: A DataFrame containing the vendor ID, total trips executed for each vendor, amount of trips whose
            duration greater than 30 minutes and percentage of these trips.
        """
        dataframe = (self.trip_data_df.filter(f.col(columns.vendor_id) != 'None')
                                      .groupBy(columns.vendor_id)
                                      .agg(f.count("*").alias("total_trips"),
                                           f.count(f.when(f.col(columns.trip_time_in_secs) > 1800, True))
                                           .alias("long_trips"))
                                      .withColumn("percentage_long_trips",
                                                  f.format_number((f.col("long_trips") /
                                                                   f.col("total_trips")) * 100, 2)))

        return dataframe

    def top_tips_in_cash(self):
        """ Calculates top 5 biggest tips for each vendor if the user paid in cash.

        Returns:
            DataFrame: A DataFrame containing the vendor ID and top 5 largest tips paid in cash for each vendor.
        """
        window_spec = Window.partitionBy(columns.vendor_id).orderBy(f.desc(columns.tip_amount))
        dataframe = (self.trip_fare_df.filter(f.col(columns.payment_type) == "CSH")
                                      .withColumn("rank", f.dense_rank().over(window_spec))
                                      .filter(f.col("rank") <= 5).select(columns.vendor_id, columns.tip_amount, "rank"))

        return dataframe

    def trips_weekdays_weekend(self):
        """ Calculates the number of trips occurred on weekend and weekdays for each vendor.

        Returns:
            DataFrame: A DataFrame containing the number of trips executed on weekdays and weekends for each vendor.
        """
        weekdays = [2, 3, 4, 5, 6]

        dataframe = self.trip_fare_df.withColumn("day_of_week", f.dayofweek(f.col(columns.pickup_datetime)))
        dataframe = (dataframe.withColumn("day_type", f.when(f.col("day_of_week")
                                                              .isin(weekdays), "weekday").otherwise("weekend"))
                              .groupBy(columns.vendor_id, "day_type")
                              .count()
                              .orderBy(columns.vendor_id, "day_type"))

        return dataframe
