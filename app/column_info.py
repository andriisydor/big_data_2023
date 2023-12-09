""" Contains tools to analise dataset columns. """


def timestamp_column_min_and_max(dataframe, column_name):
    min_timestamp = dataframe.select(column_name).agg({column_name: 'min'}).head()[0]
    max_timestamp = dataframe.select(column_name).agg({column_name: 'max'}).head()[0]
    return min_timestamp, max_timestamp


def number_of_unique_values_of_column(dataframe, column_name):
    unique_values = dataframe.select(column_name).distinct()
    return unique_values.count()
