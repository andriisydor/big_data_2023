
def describe_numeric_col(df, col_name):
    column_summary = df.select(col_name).summary()  # collects main info about column (min, max, avg, etc.)
    column_summary.show()
    nulls_count = df.filter(df[col_name].isNull()).count()  # counts number of null vals
    print(f'Number of null vals in {col_name} = {nulls_count}')


def describe_string_col(df, col_name):
    unique_values_counts = (df.groupBy(col_name).count())   # finds all unique vals and counts them in df
    unique_values_counts.show(truncate=False)
    print(f'Number of unique {col_name} = {unique_values_counts.count()}')  # counts number of unique vals
    nulls_count = df.filter(df[col_name].isNull()).count()  # counts number of null vals
    print(f'Number of null vals in {col_name} = {nulls_count}')
