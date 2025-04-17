#!/usr/bin/env python
# coding: utf-8

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
import argparse

def main(params):

    input_green = params.input_green
    input_yellow = params.input_yellow
    output = params.output
    bucket_name = 'dezoomcamp2024_project'
    # spark_sql = (
    #     SparkSession.builder \
    #         .master("spark://dezoomcamp2024.us-east1-b.c.dezoomcamp2024-week5.internal:7077").appName("App3_local_cluster_python")
    #     .config(
    #         "spark.jars.packages",
    #         "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.15.1-beta,com.google.cloud.bigdataoss:gcs-connector:hadoop2-2.1.6",
    #     )
    #     .config(
    #         "spark.jars",
    #         "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar",
    #     )
    #     .getOrCreate()
    # )
    
    spark_sql = SparkSession.builder \
        .appName("App6_local_cluster_python") \
        .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar") \
        .config(
            "spark.jars.packages",
            "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.15.1-beta,com.google.cloud.bigdataoss:gcs-connector:hadoop2-2.1.6"
        ) \
        .getOrCreate()

    # spark_sql = SparkSession.builder \
    #     .appName("App5_local_cluster_python") \
    #     .getOrCreate()
        
    spark_sql._jsc.hadoopConfiguration().set(
        "fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
    )
    spark_sql.conf.set("temporaryGcsBucket", bucket_name)

    df_green = spark_sql.read.parquet(input_green)

    df_yellow = spark_sql.read.parquet(input_yellow)

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
        
    spark_sql.stop()
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--input_green', required=True, help='input path for green_2020 parquet dataset')
    parser.add_argument('--input_yellow', required=True, help='input path for yellow_2020 parquet dataset')
    parser.add_argument('--output', required=True, help='output path for green+yellow_2020 parquet dataset')

    args = parser.parse_args()

    main(args)
        


