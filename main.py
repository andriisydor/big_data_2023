from pyspark.sql import SparkSession
from pyspark import SparkConf

from settings import DATA_DIRECTORY_PATH, TRIP_FARE_PATH, TRIP_DATA_PATH
from schemas import trip_data_schema, trip_fare_schema

from app.CSVManager import CSVManager
from app.column_info import print_trip_fare_info
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
    print_trip_fare_info(trip_fare_df)

    trip_data_df = csv_manager.read(TRIP_DATA_PATH, trip_data_schema)
    print(trip_data_df.schema)
    trip_data_df.show(20)


if __name__ == '__main__':
    main()
