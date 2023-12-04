from pyspark.sql import SparkSession
from pyspark import SparkConf

from settings import DATA_DIRECTORY_PATH, TRIP_FARE_PATH, TRIP_DATA_PATH
from app.CSVManager import CSVManager
from app.QueryManager import QueryManager
from schemas import trip_data_schema, trip_fare_schema


def main():
    spark_session = (SparkSession.builder
                     .master('local')
                     .appName('big_data_2023')
                     .config(conf=SparkConf())
                     .getOrCreate())

    csv_manager = CSVManager(spark_session, DATA_DIRECTORY_PATH)
    query_manager = QueryManager(spark_session)
 # TO DO обробити дані
    trip_fare_df = csv_manager.read(TRIP_FARE_PATH, trip_fare_schema)
    trip_fare_df = trip_fare_df.fillna({"vendor_id": "None"})
    trips_fare_by_week = query_manager.trips_count(trip_fare_df, "pickup_datetime")
    trips_fare_by_week.show()

    # trip_data_df = csv_manager.read(TRIP_DATA_PATH, trip_data_schema)
    #
    # trips_data_by_week = query_manager.trips_count(trip_data_df, "pickup_datetime")
    # trips_data_by_week.show()


if __name__ == '__main__':
    main()
