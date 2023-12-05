from pyspark.sql import SparkSession
from pyspark import SparkConf
from settings import DATA_DIRECTORY_PATH, TRIP_FARE_PATH, TRIP_DATA_PATH
from schemas import trip_data_schema, trip_fare_schema

from app.CSVManager import CSVManager
from app import columns
from app.column_info import timestamp_column_min_and_max, show_column_summary, get_null_count, show_unique_vals, \
                            number_of_unique_values_of_column, show_null_rows, show_string_column_info, \
                            show_digit_column_info, show_dataframe_main_info, show_datetime_column_info
from app.transformations import strip_names_of_columns, clean_dataframe


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

    # apply transformation: remove dublicates and rows with NULL values
    trip_fare_df = clean_dataframe(trip_fare_df)


if __name__ == '__main__':
    main()
