""" Contains tools to analise dataset columns. """

import pyspark.sql.functions as f


def show_dataframe_main_info(dataframe, number_of_rows_to_show):
    print(f'Rows count: {dataframe.count()}')
    dataframe.show(number_of_rows_to_show)
    print(f'Dataframe schema: {dataframe.schema}')
    dataframe.describe().show()
    dataframe.summary().show()


def show_datetime_column_info(dataframe, column_name):
    min_val, max_val = timestamp_column_min_and_max(dataframe, column_name)
    print(f'{column_name} min: {min_val}, max: {max_val}')


def show_digit_column_info(dataframe, column_name):
    show_column_summary(dataframe, column_name)
    null_count = get_null_count(dataframe, column_name)
    print(f'Number of null vals in {column_name}: {null_count}')


def show_string_column_info(dataframe, column_name):
    show_unique_vals(dataframe, column_name)
    null_count = get_null_count(dataframe, column_name)
    print(f'Number of null vals in {column_name}: {null_count}')
    unique_count = number_of_unique_values_of_column(dataframe, column_name)
    print(f'Number of unique vals in {column_name}: {unique_count}')


def show_column_summary(dataframe, column_name):
    column_summary = dataframe.select(column_name).summary()  # collects main info about column (min, max, avg, etc.)
    column_summary.show()


def get_null_count(dataframe, column_name):
    null_count = dataframe.filter(dataframe[column_name].isNull()).count()  # counts number of null vals
    return null_count


def show_null_rows(dataframe, column_name):
    dataframe.filter(f.col(column_name).isNull()).show()


def show_unique_vals(dataframe, column_name):
    unique_values_counts = (dataframe.groupBy(column_name).count())  # finds all unique vals and counts them in df
    unique_values_counts.show(truncate=False)


def timestamp_column_min_and_max(dataframe, column_name):
    min_timestamp = dataframe.select(column_name).agg({column_name: 'min'}).head()[0]
    max_timestamp = dataframe.select(column_name).agg({column_name: 'max'}).head()[0]
    return min_timestamp, max_timestamp


def number_of_unique_values_of_column(dataframe, column_name):
    unique_values = dataframe.select(column_name).distinct()
    return unique_values.count()
