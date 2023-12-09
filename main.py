from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import col
from settings import DATA_DIRECTORY_PATH, TRIP_FARE_PATH, TRIP_DATA_PATH
from schemas import trip_data_schema, trip_fare_schema

from app.CSVManager import CSVManager
from app import columns
from app.column_info import timestamp_column_min_and_max
from app.transformations import strip_names_of_columns
from app.QueryManager import QueryManager


def business_questions(spark, trip_fare_df, trip_data_df):
    '''all business methods will be invoked here '''
    query_manager = QueryManager(spark, trip_fare_df, trip_data_df)
    ## Which day of the week has the highest number of trips?
    trips_fare_by_week = query_manager.trips_count(columns.pickup_datetime)
    # trips_fare_by_week.show(20)
    # trips_data_by_week.show(20)
    ## What is the total revenue earned by each vendor?
    # total_revenue = query_manager.total_revenue()
    # total_revenue.show()
    ## What is the average trip distance for different passenger counts?
    # query_manager.avg_trip_distance()
    ## How many simultaneous trips happened during a day
    # simultaneous_trips = query_manager.simultaneous_trips()
    # simultaneous_trips.show()
    ## Top 5 most expensive trips
    # query_manager.expensive_trips()
    ##
    query_manager.avg_amount_rate_code()


def info(trip_fare_df):
    ''' Prints info of trip_fare_df'''
    number_of_rows_to_show = 20
    print(f'Rows count: {trip_fare_df.count()}')
    trip_fare_df.show(number_of_rows_to_show)
    print(f'Dataframe schema: {trip_fare_df.schema}')
    trip_fare_df.describe().show()
    trip_fare_df.summary().show()
    pickup_datetime_min, pickup_datetime_max = (
        timestamp_column_min_and_max(trip_fare_df, columns.pickup_datetime))
    print(f'pickup_datetime min: {pickup_datetime_min}, max: {pickup_datetime_max}')


def main():
    spark_session = (SparkSession.builder
                     .master('local')
                     .appName('big_data_2023')
                     .config(conf=SparkConf())
                     .getOrCreate())

    csv_manager = CSVManager(spark_session, DATA_DIRECTORY_PATH)

    trip_fare_df = csv_manager.read(TRIP_FARE_PATH, trip_fare_schema)
    trip_fare_df = strip_names_of_columns(trip_fare_df)
    # info(trip_fare_df)
    trip_data_df = csv_manager.read(TRIP_DATA_PATH, trip_data_schema)
    trip_fare_df = trip_fare_df.dropDuplicates()
    trip_fare_df = trip_fare_df.dropna()
    business_questions(spark_session, trip_fare_df, trip_data_df)


if __name__ == '__main__':
    main()
