from pyspark.sql import SparkSession
from pyspark import SparkConf
from settings import DATA_DIRECTORY_PATH, TRIP_FARE_PATH, TRIP_DATA_PATH
from schemas import trip_data_schema, trip_fare_schema

from app.CSVManager import CSVManager
from app import columns
from app.column_info import timestamp_column_min_and_max, show_column_summary, get_null_count, show_unique_vals, \
                            number_of_unique_values_of_column, show_null_rows
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

    print(f'Rows count: {trip_fare_df.count()}')
    trip_fare_df.show(number_of_rows_to_show)
    print(f'Dataframe schema: {trip_fare_df.schema}')
    trip_fare_df.describe().show()
    trip_fare_df.summary().show()
    pickup_datetime_min, pickup_datetime_max = timestamp_column_min_and_max(trip_fare_df, columns.pickup_datetime)
    print(f'pickup_datetime min: {pickup_datetime_min}, max: {pickup_datetime_max}')

    trip_data_df = csv_manager.read(TRIP_DATA_PATH, trip_data_schema)
    print(trip_data_df.schema)
    trip_data_df.show(number_of_rows_to_show)

    show_column_summary(trip_fare_df, columns.fare_amount)
    null_count = get_null_count(trip_fare_df, columns.fare_amount)
    print(f'Number of null vals in {columns.fare_amount}: {null_count}')

    show_column_summary(trip_fare_df, columns.surcharge)
    null_count = get_null_count(trip_fare_df, columns.surcharge)
    print(f'Number of null vals in {columns.surcharge}: {null_count}')

    show_unique_vals(trip_fare_df, columns.medallion)
    null_count = get_null_count(trip_fare_df, columns.medallion)
    print(f'Number of null vals in {columns.medallion}: {null_count}')
    unique_count = number_of_unique_values_of_column(trip_fare_df, columns.medallion)
    print(f'Number of unique vals in {columns.medallion}: {unique_count}')

    show_unique_vals(trip_fare_df, columns.hack_license)
    null_count = get_null_count(trip_fare_df, columns.hack_license)
    print(f'Number of null vals in {columns.hack_license}: {null_count}')
    unique_count = number_of_unique_values_of_column(trip_fare_df, columns.hack_license)
    print(f'Number of unique vals in {columns.hack_license}: {unique_count}')

    # apply transformation: remove dublicates and rows with NULL values
    show_null_rows(trip_fare_df, columns.fare_amount)
    trip_fare_df = clean_dataframe(trip_fare_df)
    print(f'Rows count: {trip_fare_df.count()}')


if __name__ == '__main__':
    main()
