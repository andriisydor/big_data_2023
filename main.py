from pyspark.sql import SparkSession
from pyspark import SparkConf
from settings import DATA_DIRECTORY_PATH, TRIP_FARE_PATH, TRIP_DATA_PATH
from schemas import trip_data_schema, trip_fare_schema
from app.CSVManager import CSVManager
from app import columns
from app.column_info import show_string_column_info, show_digit_column_info, \
                            show_dataframe_main_info, show_datetime_column_info
from app.transformations import strip_names_of_columns, clean_dataframe
from app.QueryManager import QueryManager


def business_questions(spark, trip_fare_df, trip_data_df):
    """all business methods will be invoked here """
    query_manager = QueryManager(spark, trip_fare_df, trip_data_df)
    ## Which day of the week has the highest number of trips?
    trips_by_week = query_manager.trips_count(columns.pickup_datetime)
    trips_by_week.show(20)
    ## What is the total revenue earned by each vendor?
    total_revenue = query_manager.total_revenue()
    total_revenue.show()
    ## What is the average trip distance for different passenger counts?
    query_manager.avg_trip_distance()
    ## How many simultaneous trips happened during a day
    simultaneous_trips = query_manager.simultaneous_trips()
    simultaneous_trips.show()
    ## Top 5 most expensive trips
    dataframe = query_manager.most_expensive_trips()
    dataframe.show()
    ## Trips which tip amount was greater than average based on rate code
    dataframe = query_manager.avg_amount_rate_code()
    dataframe.show()


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

    business_questions(spark_session, trip_fare_df, trip_data_df)


if __name__ == '__main__':
    main()
