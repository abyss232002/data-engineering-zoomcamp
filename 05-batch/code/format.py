from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType, DoubleType, IntegerType

StructType([
    StructField('dispatching_base_num', StringType(), True),
    StructField('pickup_datetime', TimestampType(), True),
    StructField('dropOff_datetime', TimestampType(), True),
    StructField('PUlocationID', IntegerType(), True),
    StructField('DOlocationID', IntegerType(), True),
    StructField('SR_Flag', StringType(), True),
    StructField('Affiliated_base_number', StringType(), True)
])

StructType([
    StructField('hvfhs_license_num', StringType(), True),
    StructField('dispatching_base_num', StringType(), True),
    StructField('originating_base_num', StringType(), True),
    StructField('request_datetime', TimestampType(), True),
    StructField('on_scene_datetime', TimestampType(), True),
    StructField('pickup_datetime', TimestampType(), True),
    StructField('dropoff_datetime', TimestampType(), True),
    StructField('DOLocationID', IntegerType(), True),
    StructField('PULocationID', IntegerType(), True),
    StructField('trip_miles', DoubleType(), True),
    StructField('trip_time', LongType(), True),
    StructField('base_passenger_fare', DoubleType(), True),
    StructField('tolls', DoubleType(), True),
    StructField('bcf', DoubleType(), True),
    StructField('sales_tax', DoubleType(), True),
    StructField('congestion_surcharge', DoubleType(), True),
    StructField('airport_fee', DoubleType(), True),
    StructField('tips', DoubleType(), True),
    StructField('driver_pay', DoubleType(), True),
    StructField('shared_request_flag', StringType(), True),
    StructField('shared_match_flag', StringType(), True),
    StructField('access_a_ride_flag', StringType(), True),
    StructField('wav_request_flag', StringType(), True),
    StructField('wav_match_flag', StringType(), True)
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


python3 spark_sql_avi_script.py \
    --input_green="gs://dezoomcamp2024_project/processed/green/2020/*" \
    --input_yellow="gs://dezoomcamp2024_project/processed/yellow/2020/*" \
    --output="gs://dezoomcamp2024_project/processed/revenue/trip_data_2020"
    
URL = "spark://dezoomcamp2024.us-east1-b.c.dezoomcamp2024-week5.internal:7077"

/home/abyssde232024/spark/spark-3.3.2-bin-hadoop3/bin/spark-submit \
    --master "spark://dezoomcamp2024.us-east1-b.c.dezoomcamp2024-week5.internal:7077" \
    --conf PROP="spark.jars.packages","com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.15.1-beta,com.google.cloud.bigdataoss:gcs-connector:hadoop2-2.1.6","spark.jars","https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar" \
    spark_sql_avi_script.py \
        --input_green="gs://dezoomcamp2024_project/processed/green/2021/*" \
        --input_yellow="gs://dezoomcamp2024_project/processed/yellow/2021/*" \
        --output="gs://dezoomcamp2024_project/processed/revenue/trip_data_2021"
        
        python3 spark_sql_avi_script.py \
    --input_green="gs://dezoomcamp2024_project/processed/green/2021/*" \
    --input_yellow="gs://dezoomcamp2024_project/processed/yellow/2021/*" \
    --output="gs://dezoomcamp2024_project/processed/revenue/trip_data_2021"
    
    
/home/abyssde232024/spark/spark-3.3.2-bin-hadoop3/bin/spark-submit \
    --master "spark://dezoomcamp2024.us-east1-b.c.dezoomcamp2024-week5.internal:7077" \
    --conf PROP="spark.jars.packages","com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.15.1-beta,com.google.cloud.bigdataoss:gcs-connector:hadoop2-2.1.6","spark.jars","https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar", \
    spark_sql_avi_script.py \
        --input_green="gs://dezoomcamp2024_project/processed/green/2021/*" \
        --input_yellow="gs://dezoomcamp2024_project/processed/yellow/2021/*" \
        --output="gs://dezoomcamp2024_project/processed/revenue/trip_data_2021"

/home/abyssde232024/spark/spark-3.3.2-bin-hadoop3/bin/spark-submit \
    --master "spark://dezoomcamp2024.us-east1-b.c.dezoomcamp2024-week5.internal:7077" \
    spark_sql_avi_script.py \
        --input_green="data/processed/green/2021/*" \
        --input_yellow="data/processed/yellow/2021/*" \
        --output="data/report/revenue/trip_data_2021"
        
/home/abyssde232024/spark/spark-3.3.2-bin-hadoop3/bin/spark-submit \
    --master "spark://dezoomcamp2024.us-east1-b.c.dezoomcamp2024-week5.internal:7077" \
    spark_sql_avi_script.py \
        --input_green="gs://dezoomcamp2024_project/processed/green/2021/*/" \
        --input_yellow="gs://dezoomcamp2024_project/processed/yellow/2021/*/" \
        --output="gs://dezoomcamp2024_project/processed/revenue/trip_data_2021"
        
    --input_green="processed/green/2020/" \
    --input_yellow="processed/yellow/2020/" \
    --output="gs://dezoomcamp2024_project/processed/revenue/trip_data_2020"
    
        --input_green="gs://dezoomcamp2024_project/pq/green/2020/*/"
        --input_yellow="gs://dezoomcamp2024_project/pq/yellow/2020/*/"
        --output="gs://dezoomcamp2024_project/pq/revenue/trip_data_2020_dp"
