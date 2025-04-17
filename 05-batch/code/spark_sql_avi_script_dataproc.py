#!/usr/bin/env python
# coding: utf-8

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType, DoubleType, IntegerType
import os
import argparse
from google.cloud import storage

def main(params):

    prefix_green = params.input_green
    prefix_yellow = params.input_yellow
    output = params.output
    bucket_name = 'dezoomcamp2024_project'
    
    # Pyspark script.Job will be submitted via dataproc managed version of spark 
    
    spark_sql = SparkSession.builder \
        .appName("App1_dataproc_cluster_python") \
        .getOrCreate()
    
    green_schema = StructType([
    StructField("VendorID", IntegerType(), nullable=True),
    StructField("lpep_pickup_datetime", TimestampType(), nullable=True),
    StructField("lpep_dropoff_datetime", TimestampType(), nullable=True),
    StructField("store_and_fwd_flag", StringType(), nullable=True),
    StructField("RatecodeID", IntegerType(), nullable=True),
    StructField("PULocationID", IntegerType(), nullable=True),
    StructField("DOLocationID", IntegerType(), nullable=True),
    StructField("passenger_count", IntegerType(), nullable=True),
    StructField("trip_distance", DoubleType(), nullable=True),
    StructField("fare_amount", DoubleType(), nullable=True),
    StructField("extra", DoubleType(), nullable=True),
    StructField("mta_tax", DoubleType(), nullable=True),
    StructField("tip_amount", DoubleType(), nullable=True),
    StructField("tolls_amount", DoubleType(), nullable=True),
    StructField("ehail_fee", DoubleType(), nullable=True),
    StructField("improvement_surcharge", DoubleType(), nullable=True),
    StructField("total_amount", DoubleType(), nullable=True),
    StructField("payment_type", IntegerType(), nullable=True),
    StructField("trip_type", IntegerType(), nullable=True),
    StructField("congestion_surcharge", DoubleType(), nullable=True)
    ])
    yellow_schema = StructType([
    StructField("VendorID", IntegerType(), nullable=True),
    StructField("tpep_pickup_datetime", TimestampType(), nullable=True),
    StructField("tpep_dropoff_datetime", TimestampType(), nullable=True),
    StructField("passenger_count", IntegerType(), nullable=True),
    StructField("trip_distance", DoubleType(), nullable=True),
    StructField("RatecodeID", IntegerType(), nullable=True),
    StructField("store_and_fwd_flag", StringType(), nullable=True),
    StructField("PULocationID", IntegerType(), nullable=True),
    StructField("DOLocationID", IntegerType(), nullable=True),
    StructField("payment_type", IntegerType(), nullable=True),
    StructField("fare_amount", DoubleType(), nullable=True),
    StructField("extra", DoubleType(), nullable=True),
    StructField("mta_tax", DoubleType(), nullable=True),
    StructField("tip_amount", DoubleType(), nullable=True),
    StructField("tolls_amount", DoubleType(), nullable=True),
    StructField("improvement_surcharge", DoubleType(), nullable=True),
    StructField("total_amount", DoubleType(), nullable=True),
    StructField("congestion_surcharge", DoubleType(), nullable=True)
    ])

    # Initialize GCS client
    client = storage.Client()

    # Specify your GCS bucket and prefix
    bucket_name = "dezoomcamp2024_project"
    prefix_green = "processed/green/2020/"
    prefix_yellow = "processed/yellow/2020/"

    # List all files in the bucket with the specified prefix
    blobs = client.list_blobs(bucket_name, prefix=prefix_green)

    # Read each file individually
    for blob in blobs:
        file_path = f"gs://{bucket_name}/{blob.name}"
        print(f"Reading file: {file_path}")
        if blob.name != '._SUCCESS.crc':
            df_green = spark_sql.read.parquet(file_path, schema=green_schema)

    # List all yellow files in the bucket with the specified prefix
    blobs = client.list_blobs(bucket_name, prefix=prefix_yellow)

    # Read each file individually
    for blob in blobs:
        file_path = f"gs://{bucket_name}/{blob.name}"
        print(f"Reading file: {file_path}")
        if blob.name != '._SUCCESS.crc':
            df_yellow = spark_sql.read.parquet(file_path, schema=yellow_schema)
        
    print(f"Green count-->{df_green.count()}")
    print(f"Yellow count-->{df_yellow.count()}")

    df_yellow = df_yellow \
        .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime') \
        .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime')
    df_green = df_green \
        .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime') \
        .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime')
    #  create a common column list which is present in both green and yellow dataframes and order them same as green dataframe
    common_columns = []
    yellow_columns = df_yellow.columns
    for column in df_green.columns:
        if column in yellow_columns:
            common_columns.append(column)

    # add service type column to green dataframe and yellow dataframe
    df_green = df_green.withColumn('service_type', F.lit('green'))    
    df_yellow = df_yellow.withColumn('service_type', F.lit('yellow'))

    # select only common columns and service type column from green and yellow dataframes
    df_yellow_select = df_yellow.select(common_columns + ['service_type'])

    df_green_select = df_green.select(common_columns + ['service_type'])

    # union both green and yellow dataframes
    df_trip_data = df_green_select.unionAll(df_yellow_select)

    # create temporary table from the dataframe
    df_trip_data.registerTempTable('trip_data')

    df_result_2020 = spark_sql.sql('''
    select 
        -- Reveneue grouping 
        PULocationID as revenue_zone,
        date_trunc("month", "pickup_datetime") as revenue_month, 
        service_type, 

        -- Revenue calculation 
        sum(fare_amount) as revenue_monthly_fare,
        sum(extra) as revenue_monthly_extra,
        sum(mta_tax) as revenue_monthly_mta_tax,
        sum(tip_amount) as revenue_monthly_tip_amount,
        sum(tolls_amount) as revenue_monthly_tolls_amount,
        sum(improvement_surcharge) as revenue_monthly_improvement_surcharge,
        sum(total_amount) as revenue_monthly_total_amount,

        -- Additional calculations
        avg(passenger_count) as avg_monthly_passenger_count,
        avg(trip_distance) as avg_monthly_trip_distance

    from trip_data
    group by 1,2,3
    ''')
    # Print output to console
    print(df_result_2020.show())

    try:
        df_result_2020.repartition(4).write.parquet(output, mode='overwrite')
    except Exception as e:
        print(f"Gave error while writing: :{e}")
    else:
        print("Successfully written in gs://dezoomcamp2024_project/processed/revenue/trip_data_2020")
        
    # spark_sql.stop()
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--input_green', required=True, help='input path for green_2020 parquet dataset')
    parser.add_argument('--input_yellow', required=True, help='input path for yellow_2020 parquet dataset')
    parser.add_argument('--output', required=True, help='output path for green+yellow_2020 parquet dataset')

    args = parser.parse_args()

    main(args)
        


