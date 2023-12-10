from pyspark.sql import Window
from  pyspark.sql.functions import col, date_format, desc, max, sum, format_number, avg, lit, asc
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
                                       date_format(self.trip_data_df[date_column], 'EEEE'))
        trips_by_week = (trip_df.filter(col(columns.vendor_id) != 'None').groupBy(columns.vendor_id, 'dayofweek').
                         count().orderBy(desc(columns.vendor_id), desc('count')).withColumn('max_trip_count',
                                                                                            max('count').over(
                                                                                                Window.partitionBy(
                                                                                                    'vendor_id')))
                         .filter(col('count') == col('max_trip_count')).drop('max_trip_count'))
        return trips_by_week

    def total_revenue(self):
        """ Calculates the total revenue of each vendor """
        dataframe = (self.trip_fare_df.filter(col(columns.vendor_id) != 'None').groupBy(columns.vendor_id)
                     .agg(format_number(sum(columns.total_amount), 2).alias('total revenue')))
        return dataframe

    def avg_trip_distance(self):
        """ Calculates the average trip distance for different amount of passengers """
        dataframe = (self.trip_data_df.filter(col(columns.passenger_count).
                                              isNotNull()).groupBy(columns.vendor_id, columns.passenger_count).
                     agg(avg(columns.trip_distance)).orderBy(desc(columns.passenger_count)))
        return dataframe

    def simultaneous_trips(self):
        """ Calculates how many trips happened simultaneously """
        pickup_dataframe = (self.trip_data_df.filter(col(columns.pickup_datetime).isNotNull()).
                            select(col(columns.pickup_datetime).alias('event_time'),
                                   lit(1).alias('event_count')))
        dropoff_dateframe = (self.trip_data_df.filter(col(columns.dropoff_datetime).isNotNull()).
                             select(col(columns.dropoff_datetime).alias('event_time'),
                                    lit(-1).alias('event_count')))
        event_dateframe = pickup_dataframe.union(dropoff_dateframe)
        dataframe = event_dateframe.withColumn('sum', sum('event_count').over(Window.partitionBy('event_time')
                                                                              .orderBy(asc('event_time'))))
        dataframe = dataframe.groupBy(date_format('event_time', 'yyyy-MM-dd').alias('day')
                                      ).agg(max('sum').alias('simultaneous_trips')).orderBy(
            desc(col('simultaneous_trips'))).limit(5)
        return dataframe

    def most_expensive_trips(self):
        """Calculates the most expensive trips by vendor """
        dataframe = self.trip_fare_df.groupBy(columns.vendor_id).agg(max(columns.total_amount).
                                                                     alias(columns.total_amount))
        return dataframe

    def avg_amount_rate_code(self):
        """ Calculates count of trips the tip above average tip amount for trips with different rate code. """
        dataframe = self.trip_fare_df.join(
                    self.trip_data_df,
                    [self.trip_data_df[columns.medallion] == self.trip_fare_df[columns.medallion],
                     self.trip_data_df[columns.hack_license] == self.trip_fare_df[columns.hack_license],
                     self.trip_data_df[columns.pickup_datetime] == self.trip_fare_df[columns.pickup_datetime]],
                    'inner'
                )
        average_tip_amounts = dataframe.groupBy(columns.rate_code).agg(avg(columns.tip_amount).alias('avg_tip_amount'))
        joined_data = dataframe.join(average_tip_amounts, on=columns.rate_code, how='inner')
        dataframe = joined_data.withColumn('tip_above_avg', col('tip_amount') > col('avg_tip_amount'))
        dataframe = (dataframe.groupBy(columns.rate_code).count().withColumnRenamed('count', 'trip_count').
                     orderBy(desc('trip_count')))
        return dataframe


