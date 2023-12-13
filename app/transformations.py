""" Contains transformation functions for dataframes. """


def strip_names_of_columns(dataframe):
    """Returns dataframe with removed spaces at the start and at the end of name of column of dataframe.

    Args:
        dataframe (pyspark.sql.DataFrame): dataframe which columns will be updated.

    Returns:
        pyspark.sql.DataFrame
    """

    correct_names_of_columns = [name.strip() for name in dataframe.columns]
    for old_name, new_name in zip(dataframe.columns, correct_names_of_columns):
        dataframe = dataframe.withColumnRenamed(old_name, new_name)
    return dataframe


def clean_dataframe(dataframe):
    """
    Removes duplicate rows and rows with NULL values from the dataframe.

    Args:
        dataframe (pyspark.sql.DataFrame): dataframe which rows will be dropped.

    Returns:
        pyspark.sql.DataFrame
    """
    dataframe = dataframe.dropDuplicates()
    dataframe = dataframe.na.drop("all")

    return dataframe
