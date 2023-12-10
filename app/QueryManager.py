from pyspark.sql import Window
import pyspark.sql.functions as F
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
                                               F.date_format(self.trip_data_df[date_column], 'EEEE'))
        trips_by_week = (trip_df.filter(F.col(columns.vendor_id) != 'None').groupBy(columns.vendor_id, 'dayofweek').
                         count().orderBy(F.desc(columns.vendor_id), F.desc('count')).withColumn('max_trip_count',
                                                                                            F.max('count').over(
                                                                                                Window.partitionBy(
                                                                                                    'vendor_id')))
                         .filter(F.col('count') == F.col('max_trip_count')).drop('max_trip_count'))
        return trips_by_week

    def total_revenue(self):
        """ Calculates the total revenue of each vendor
             Returns:
            DataFrame: A DataFrame containing the total revenue for each vendor.
        """
        dataframe = (self.trip_fare_df.filter(F.col(columns.vendor_id) != 'None').groupBy(columns.vendor_id)
                     .agg(F.format_number(F.sum(columns.total_amount), 2).alias('total revenue')))
        return dataframe

    def avg_trip_distance(self):
        """
            Calculates the average trip distance for different numbers of passengers.

            Returns:
                DataFrame: A DataFrame containing the average trip distance for each combination of vendor and passenger count.
        """
        dataframe = (self.trip_data_df.filter(F.col(columns.passenger_count).
                                              isNotNull()).groupBy(columns.vendor_id, columns.passenger_count).
                     agg(F.avg(columns.trip_distance)).orderBy(F.desc(columns.passenger_count)))
        return dataframe

    def simultaneous_trips(self):
        """
        Calculates the maximum number of simultaneous trips that happened on the same day.

        Returns:
            DataFrame: A DataFrame containing the maximum number of simultaneous trips for the top 5 days.
        """
        pickup_dataframe = (self.trip_data_df.filter(F.col(columns.pickup_datetime).isNotNull()).
                            select(F.col(columns.pickup_datetime).alias('event_time'),
                                   F.lit(1).alias('event_count')))
        dropoff_dateframe = (self.trip_data_df.filter(F.col(columns.dropoff_datetime).isNotNull()).
                             select(F.col(columns.dropoff_datetime).alias('event_time'),
                                    F.lit(-1).alias('event_count')))
        event_dateframe = pickup_dataframe.union(dropoff_dateframe)
        dataframe = event_dateframe.withColumn('sum', F.sum('event_count').over(Window.partitionBy('event_time')
                                                                              .orderBy(F.asc('event_time'))))
        dataframe = dataframe.groupBy(F.date_format('event_time', 'yyyy-MM-dd').alias('day')
                                      ).agg(F.max('sum').alias('simultaneous_trips')).orderBy(
            F.desc(F.col('simultaneous_trips'))).limit(5)
        return dataframe

    def most_expensive_trips(self):
        """
        Calculates the most expensive trips for each vendor.

        Returns:
            DataFrame: A DataFrame containing the most expensive trips for each vendor.
        """
        dataframe = self.trip_fare_df.groupBy(columns.vendor_id).agg(F.max(columns.total_amount).
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
        average_tip_amounts = dataframe.groupBy(columns.rate_code).agg(F.avg(columns.tip_amount).alias('avg_tip_amount'))
        joined_data = dataframe.join(average_tip_amounts, on=columns.rate_code, how='inner')
        dataframe = joined_data.withColumn('tip_above_avg', F.col('tip_amount') > F.col('avg_tip_amount'))
        dataframe = (dataframe.groupBy(columns.rate_code).count().withColumnRenamed('count', 'trip_count').
                     orderBy(F.desc('trip_count')))
        return dataframe
