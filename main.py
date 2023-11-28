from pyspark.sql import SparkSession
from pyspark import SparkConf
from settings import DATA_DIRECTORY_PATH, TRIP_FARE_PATH, TRIP_DATA_PATH
from app.CSVManager import CSVManager
from schemas import trip_data_schema, trip_fare_schema


def main():
    spark_session = (SparkSession.builder
                     .master('local')
                     .appName('big_data_2023')
                     .config(conf=SparkConf())
                     .getOrCreate())

    csv_manager = CSVManager(spark_session, DATA_DIRECTORY_PATH)

    trip_fare_df = csv_manager.read(TRIP_FARE_PATH, trip_fare_schema)
    trip_fare_df.show(20)
    print(trip_fare_df.schema)

    trip_data_df = csv_manager.read(TRIP_DATA_PATH, trip_data_schema)
    print(trip_data_df.schema)
    trip_data_df.show(20)


if __name__ == '__main__':
    main()
