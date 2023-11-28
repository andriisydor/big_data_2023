""" Schemas for nyc datasets """
from pyspark.sql.types import (StructType, StructField,
                               StringType, TimestampType, DoubleType, IntegerType)

trip_fare_schema = StructType([
        StructField("medallion", StringType(), True),
        StructField("hack_license", StringType(), True),
        StructField("vendor_id", StringType(), True),
        StructField("pickup_datetime", TimestampType(), True),
        StructField("payment_type", StringType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("surcharge", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("total_amount", DoubleType(), True)
    ])


trip_data_schema = schema = StructType([
        StructField("medallion", StringType(), True),
        StructField("hack_license", StringType(), True),
        StructField("vendor_id", StringType(), True),
        StructField("rate_code", IntegerType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("pickup_datetime", TimestampType(), True),
        StructField("dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("trip_time_in_secs", DoubleType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("pickup_longitude", DoubleType(), True),
        StructField("pickup_latitude", DoubleType(), True),
        StructField("dropoff_longitude", DoubleType(), True),
        StructField("dropoff_latitude", DoubleType(), True)
    ])
