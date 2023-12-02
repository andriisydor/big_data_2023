""" Contains tools to analise dataset columns. """
from app import columns


def gather_and_print_dataframe_info(dataframe):
    """Prints some analytical information about dataframe.

    Args:
        dataframe (pyspark.sql.DataFrame): Dataframe for analysis.
    """

    number_of_rows_to_show = 20
    print(f'Rows count: {dataframe.count()}')
    print(f'First {number_of_rows_to_show} rows:')
    dataframe.show(number_of_rows_to_show)
    print(f'Dataframe schema: {dataframe.schema}')
    print('Columns description: ')
    dataframe.describe().show()
    print('Columns summary: ')
    dataframe.summary().show()


def print_timestamp_column_min_and_max(dataframe, column_name):
    dataframe.select(column_name).agg({column_name: "min"}).show()
    dataframe.select(column_name).agg({column_name: "max"}).show()


def print_trip_fare_info(trip_fare_dataframe):
    gather_and_print_dataframe_info(trip_fare_dataframe)
    print_timestamp_column_min_and_max(trip_fare_dataframe, columns.pickup_datetime)
