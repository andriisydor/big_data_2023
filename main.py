from pyspark.sql import SparkSession
from pyspark import SparkConf

from settings import DATA_DIRECTORY_PATH, TRIP_FARE_PATH, TRIP_DATA_PATH
from app.read_write import CSVManager


def main():
    spark_session = (SparkSession.builder
                     .master('local')
                     .appName('big_data_2023')
                     .config(conf=SparkConf())
                     .getOrCreate())

    csv_manager = CSVManager(spark_session, DATA_DIRECTORY_PATH)

    trip_fare_df = csv_manager.read(TRIP_FARE_PATH)
    trip_fare_df.show()
    print(trip_fare_df.columns)

    trip_data_df = csv_manager.read(TRIP_DATA_PATH)
    trip_data_df.show()
    print(trip_data_df.columns)


if __name__ == '__main__':
    main()
