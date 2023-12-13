from pyspark.sql import SparkSession
from pyspark import SparkConf
from settings import DATA_DIRECTORY_PATH, TRIP_FARE_PATH, TRIP_DATA_PATH, OUTPUT_DIRECTORY
from schemas import trip_data_schema, trip_fare_schema
from app.CSVManager import CSVManager
from app import columns
from app.column_info import show_string_column_info, show_digit_column_info, \
    show_dataframe_main_info, show_datetime_column_info
from app.transformations import strip_names_of_columns, clean_dataframe
from app.QueryManager import QueryManager


def business_questions(query_manager, csv_manager, output_directory):
    """all business methods will be invoked here """
    trips_by_week = query_manager.trips_count(columns.pickup_datetime)
    csv_manager.write(trips_by_week, f'{output_directory}/trips_count_by_dayofweek')

    total_revenue = query_manager.total_revenue()
    csv_manager.write(total_revenue, f'{output_directory}/total_revenue')

    avg_trip_dist_for_passengers = query_manager.avg_trip_distance()
    csv_manager.write(avg_trip_dist_for_passengers,
                      f'{output_directory}/avg_trip_dist_for_passengers')

    simultaneous_trips = query_manager.simultaneous_trips()
    csv_manager.write(simultaneous_trips, f'{output_directory}/simultaneous_trips')

    most_expensive_trips = query_manager.most_expensive_trips()
    csv_manager.write(most_expensive_trips, f'{output_directory}/most_expensive_trips')

    avg_amount_rate_code = query_manager.avg_amount_rate_code()
    csv_manager.write(avg_amount_rate_code, f'{output_directory}/avg_amount_rate_code')

    trips_with_tip_greater_than_fare = query_manager.trips_with_tip_mount_greater_than_fare_amount()
    csv_manager.write(trips_with_tip_greater_than_fare,
                      f'{output_directory}/trips_with_tip_greater_than_fare')

    earnings_of_each_vendor = (query_manager.
                               total_earnings_of_each_vendor_for_first_seven_days_of_january())
    csv_manager.write(earnings_of_each_vendor, f'{output_directory}/earnings_of_each_vendor')

    driver_of_each_day = query_manager.driver_of_each_day()
    csv_manager.write(driver_of_each_day, f'{output_directory}/driver_of_each_day')

    price_per_second_of_drive = query_manager.price_per_second_of_drive_for_each_vendor()
    csv_manager.write(price_per_second_of_drive, f'{output_directory}/price_per_second_of_drive')

    top_vendor_for_each_payment_type = query_manager.top_vendor_for_each_payment_type()
    csv_manager.write(top_vendor_for_each_payment_type,
                      f'{output_directory}/top_vendor_for_each_payment_type')

    time_in_trip_top_drivers = query_manager.top_five_drivers_with_greatest_sum_of_time_in_trip()
    csv_manager.write(time_in_trip_top_drivers, f'{output_directory}/time_in_trip_top_drivers')

    tips_by_weekday = query_manager.tips_count()
    csv_manager.write(tips_by_weekday, f'{output_directory}/tips_by_weekday')

    avg_fare_amount_by_cash_payment = query_manager.avg_fare_amount_payment()
    csv_manager.write(avg_fare_amount_by_cash_payment,
                      f'{output_directory}/avg_fare_amount_by_cash_payment')

    top_vendor_drivers = query_manager.top_vendor_drivers()
    csv_manager.write(top_vendor_drivers, f'{output_directory}/top_vendor_drivers')

    percentage_long_trips = query_manager.percentage_long_trips()
    csv_manager.write(percentage_long_trips, f'{output_directory}/percentage_long_trips')

    top_tips_in_cash = query_manager.top_tips_in_cash()
    csv_manager.write(top_tips_in_cash, f'{output_directory}/top_tips_in_cash')

    trips_weekdays_weekend = query_manager.trips_weekdays_weekend()
    csv_manager.write(trips_weekdays_weekend, f'{output_directory}/trips_weekdays_weekend')

    most_popular_payment_type = query_manager.most_popular_payment_type()
    csv_manager.write(most_popular_payment_type, f'{output_directory}/most_popular_payment_type')

    highest_fare_amount = query_manager.highest_fare_amount()
    csv_manager.write(highest_fare_amount, f'{output_directory}/highest_fare_amount')
    highest_fare_amount.show()

    top_total_amount = query_manager.top_total_amount()
    csv_manager.write(top_total_amount, f'{output_directory}/top_total_amount')

    total_revenue_per_day = query_manager.total_revenue_per_day()
    csv_manager.write(total_revenue_per_day, f'{output_directory}/total_revenue_per_day')

    tip_percentage = query_manager.tip_percentage()
    csv_manager.write(tip_percentage, f'{output_directory}/tip_percentage')

    avg_trip_duration = query_manager.avg_trip_duration()
    csv_manager.write(avg_trip_duration, f'{output_directory}/avg_trip_duration')


def main():
    spark_session = (SparkSession.builder
                     .master('local')
                     .appName('big_data_2023')
                     .config(conf=SparkConf())
                     .getOrCreate())

    csv_manager = CSVManager(spark_session, DATA_DIRECTORY_PATH)

    trip_fare_df = csv_manager.read(TRIP_FARE_PATH, trip_fare_schema)
    trip_fare_df = strip_names_of_columns(trip_fare_df)

    trip_data_df = csv_manager.read(TRIP_DATA_PATH, trip_data_schema)

    number_of_rows_to_show = 20
    show_dataframe_main_info(trip_fare_df, number_of_rows_to_show)
    show_datetime_column_info(trip_fare_df, columns.pickup_datetime)
    show_digit_column_info(trip_fare_df, columns.fare_amount)
    show_digit_column_info(trip_fare_df, columns.surcharge)
    show_string_column_info(trip_fare_df, columns.medallion)
    show_string_column_info(trip_fare_df, columns.hack_license)

    trip_fare_df = clean_dataframe(trip_fare_df)
    trip_data_df = clean_dataframe(trip_data_df)

    query_manager = QueryManager(spark_session, trip_fare_df, trip_data_df)
    business_questions(query_manager, csv_manager, OUTPUT_DIRECTORY)


if __name__ == '__main__':
    main()
