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
        trips_by_week = (trip_df.filter(f.col(columns.vendor_id).isNotNull()).groupBy(columns.vendor_id, 'dayofweek').
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
        dataframe = (self.trip_fare_df.filter(f.col(columns.vendor_id).isNotNull()).groupBy(columns.vendor_id)
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
        dataframe = (self.trip_fare_df.filter(f.col(columns.vendor_id).isNotNull())
                     .groupBy(columns.vendor_id).agg(f.max(columns.total_amount).
                                                     alias(columns.total_amount)))
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

        joined_data = dataframe.join(average_tip_amounts, [columns.rate_code], 'inner')
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
        """ Driver who received the biggest amount of tips for each day
        (tips are considered received when the trip is over).

        Returns:
            dataframe with columns:
            date, hack_licence, vendor_id, tips_sum.
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
        """ Average price per second of drive for each vendor.

        Returns:
            dataframe with columns:
            vendor_id, average_fare_per_second
        """
        column_sum_fare_amount = 'sum_fare_amount'
        column_sum_trip_time_in_secs = 'sum_trip_time_in_secs'
        column_average_fare_per_second = 'average_fare_per_second'
        join_column_names = [columns.vendor_id, columns.medallion, columns.hack_license, columns.pickup_datetime]
        joined_df = self.trip_fare_df.join(self.trip_data_df, join_column_names, 'inner')
        price_per_second_of_drive_for_each_vendor = (
            joined_df.groupBy('vendor_id')
            .agg(f.sum(columns.fare_amount).alias(column_sum_fare_amount),
                 f.sum(columns.trip_time_in_secs).alias(column_sum_trip_time_in_secs))
            .withColumn(column_average_fare_per_second,
                        f.col(column_sum_fare_amount) / f.col(column_sum_trip_time_in_secs))
            .select(columns.vendor_id, column_average_fare_per_second)
        )
        return price_per_second_of_drive_for_each_vendor

    def top_vendor_for_each_payment_type(self):
        """ Vendor who received the biggest amount of money for each payment type.

        Returns:
            dataframe with columns:
            payment_type, vendor_id, sum_total_amount.
        """
        column_sum_total_amount = 'sum_total_amount'
        column_max_for_payment_type = 'max_for_payment_type'
        top_vendor_for_each_payment_type = (
            self.trip_fare_df.groupBy(columns.vendor_id, columns.payment_type)
            .agg(f.sum(columns.total_amount).alias(column_sum_total_amount))
            .orderBy(columns.payment_type, f.desc(column_sum_total_amount))
            .withColumn(column_max_for_payment_type,
                        f.max(f.col(column_sum_total_amount))
                        .over(Window.partitionBy(columns.payment_type)))
            .filter(f.col(column_sum_total_amount) == f.col(column_max_for_payment_type))
            .select(columns.payment_type, columns.vendor_id, column_sum_total_amount)
        )
        return top_vendor_for_each_payment_type

    def top_five_drivers_with_greatest_sum_of_time_in_trip(self):
        """ Top 5 drivers with greatest sum of time spent in trips.

        Returns:
            dataframe with columns:
            vendor_id, hack_license, sum_trip_time_in_secs
        """
        column_sum_trip_time_in_secs = 'sum_trip_time_in_secs'
        top_five_drivers_with_greatest_sum_of_time_in_trip = (
            self.trip_data_df.groupBy(columns.vendor_id, columns.hack_license)
            .agg(f.sum(f.col(columns.trip_time_in_secs)).alias(column_sum_trip_time_in_secs))
            .orderBy(f.desc(column_sum_trip_time_in_secs))
        ).limit(5)

        return top_five_drivers_with_greatest_sum_of_time_in_trip

    def most_popular_payment_type(self):
        """
        Calculates the most popular payment type.

        Returns:
            DataFrame: A DataFrame containing only one row with the most popular payment type.
        """

        dataframe = (
            self.trip_fare_df.groupBy(columns.payment_type)
            .count()
            .orderBy('count', ascending=False)
            .limit(1)
        )

        return dataframe

    def highest_fare_amount(self):
        """
        Calculates the highest fare when vendor is VTS.

        Returns:
            DataFrame: A DataFrame containing only one row with the highest fare amount for VTS.
        """

        dataframe = (
            self.trip_fare_df.filter(f.col(columns.vendor_id) == 'VTS')
            .orderBy(columns.fare_amount, ascending=False)
            .limit(1)
        )

        return dataframe

    def top_total_amount(self):
        """
        Calculates the top 10 total_amount values for drivers when passengers count > 5.

        Returns:
            DataFrame: A DataFrame containing 10 rows with biggest total_amount values for drivers
            when passengers count > 5.
        """
        dataframe = (
            self.trip_fare_df.join(self.trip_data_df, [columns.medallion, columns.hack_license,
                                                       columns.pickup_datetime], 'inner')
            .filter(f.col(columns.passenger_count) > 5)
            .groupBy(columns.medallion, columns.hack_license, columns.passenger_count)
            .agg(f.max(columns.total_amount))
            .orderBy(f.col(f'max({columns.total_amount})'), ascending=False)
            .limit(10)
        )

        return dataframe

    def total_revenue_per_day(self):
        """
        Calculates the total revenue for each day of the week, categorized by payment type.

        Returns:
            DataFrame: A DataFrame with columns: 'pickup_datetime', 'payment_type', 'total_amount',
            and 'total_revenue_per_day'.
        """

        dataframe = self.trip_fare_df.withColumn('day_num', f.dayofweek(columns.pickup_datetime))

        window_spec = (
            Window.partitionBy(
                f.col('day_num'),
                f.col(columns.payment_type)
            ).orderBy(f.col('day_num'))
        )

        dataframe = dataframe.withColumn('total_revenue_per_day', f.sum(f.col(columns.total_amount)).over(window_spec))

        return dataframe

    def tip_percentage(self):
        """
        Calculates percentage of tip to total_amount if payment type not cash.
        Returns:
            DataFrame: A DataFrame with new column tips_percentages and only rides which were paid not in cash.
        """
        window_spec = Window.partitionBy(columns.medallion, columns.hack_license, columns.pickup_datetime)

        dataframe = self.trip_fare_df.filter(f.col(columns.payment_type) != 'CSH')
        dataframe = dataframe.withColumn('tips_percetages',
                                         (f.sum(columns.tip_amount).over(window_spec) /
                                          f.sum(columns.total_amount).over(window_spec)) * 100)

        return dataframe

    def avg_trip_duration(self):
        """
        Calculates the average trip duration for different rate codes.
        Returns:
            DataFrame: A DataFrame grouped by rate codes and found avg trip duration time for them
        """
        dataframe = (
            self.trip_data_df
            .filter(f.col(columns.rate_code).isNotNull())
            .groupBy(columns.rate_code)
            .agg(
                f.avg(columns.trip_time_in_secs)
                .alias('avg_trip_duration')
            ).orderBy(f.asc(columns.rate_code))
        )

        return dataframe
