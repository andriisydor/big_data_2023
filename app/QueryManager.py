from pyspark.sql.functions import col, date_format, desc
from pyspark.sql import DataFrame


class QueryManager:
    def __init__(self, spark):
        self.spark = spark

    def trips_count(self, dataframe: DataFrame, date_column):
        dataframe = dataframe.withColumn("dayofweek",
                                         date_format(dataframe[date_column], "EEEE"))
        trips_by_week = (dataframe.filter(col("vendor_id") != "None").groupBy("vendor_id", "dayofweek").
                         count().orderBy(desc("vendor_id"), desc("count")))
        return trips_by_week
