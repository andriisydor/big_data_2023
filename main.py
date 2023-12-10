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
    # Which day of the week has the highest number of trips?
    trips_fare_by_week = query_manager.trips_count(trip_fare_df, "pickup_datetime")
    trips_data_by_week = query_manager.trips_count(trip_data_df, "pickup_datetime")
    trips_fare_by_week.show(20)
    trips_data_by_week.show(20)

    # trips_with_tip_mount_greater_than_fare_amount = query_manager.trips_with_tip_mount_greater_than_fare_amount()
    # csv_manager.write(trips_with_tip_mount_greater_than_fare_amount, f'{output_directory}/andrii/1')
    # total_earnings_of_each_vendor_for_january = \
    #     query_manager.total_earnings_of_each_vendor_for_first_seven_days_of_january()
    # csv_manager.write(total_earnings_of_each_vendor_for_january, f'{output_directory}/andrii/2')
    driver_of_each_day = query_manager.driver_of_each_day()
    csv_manager.write(driver_of_each_day, f'{output_directory}/andrii/3')


def main():
    spark_session = (SparkSession.builder
                     .master('local')
                     .appName('big_data_2023')
                     .config(conf=SparkConf())
                     .getOrCreate())

    csv_manager = CSVManager(spark_session, DATA_DIRECTORY_PATH)

    trip_fare_df = csv_manager.read(TRIP_FARE_PATH, trip_fare_schema)
    trip_fare_df = strip_names_of_columns(trip_fare_df)
    number_of_rows_to_show = 20

    trip_data_df = csv_manager.read(TRIP_DATA_PATH, trip_data_schema)

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
