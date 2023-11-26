from pyspark.sql import SparkSession
from pyspark import SparkConf

from settings import DATA_DIRECTORY_PATH, TRIP_FARE_PATH
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


if __name__ == '__main__':
    main()
