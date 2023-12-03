from pyspark.sql import SparkSession
from pyspark import SparkConf

from settings import DATA_DIRECTORY_PATH, TRIP_FARE_PATH, TRIP_DATA_PATH
from schemas import trip_data_schema, trip_fare_schema

from app.CSVManager import CSVManager
from app import columns
from app.column_info import timestamp_column_min_and_max
from app.transformations import strip_names_of_columns


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
    print(f'First {number_of_rows_to_show} rows:')
    trip_fare_df.show(number_of_rows_to_show)
    print(f'Dataframe schema: {trip_fare_df.schema}')
    print('Columns description: ')
    trip_fare_df.describe().show()
    print('Columns summary: ')
    trip_fare_df.summary().show()
    pickup_datetime_min, pickup_datetime_max = timestamp_column_min_and_max(trip_fare_df, columns.pickup_datetime)
    print(f'pickup_datetime min: {pickup_datetime_min}, max: {pickup_datetime_max}')

    trip_data_df = csv_manager.read(TRIP_DATA_PATH, trip_data_schema)
    print(trip_data_df.schema)
    trip_data_df.show(number_of_rows_to_show)


if __name__ == '__main__':
    main()
