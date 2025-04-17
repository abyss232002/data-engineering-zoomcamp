gcloud dataproc jobs submit pyspark \
    --cluster=dezoomcamp2024-cluster1 \
    --region=us-east1 \
    gs://dezoomcamp2024_project/code/spark_sql_avi_script_dataproc_v2.py \
    -- \
        --input_green=gs://dezoomcamp2024_project/pq/green/2020/*/ \
        --input_yellow=gs://dezoomcamp2024_project/pq/yellow/2020/*/ \
        --output=gs://dezoomcamp2024_project/pq/revenue/trip_data_2020_dp1

gcloud dataproc jobs submit pyspark \
    --cluster=dezoomcamp2024-cluster1 \
    --region=us-east1 \
    --jars=gs://spark-lib/bigquery/spark-3.5-bigquery-0.37.0.jar \
    gs://dezoomcamp2024_project/code/spark_sql_avi_script_dataproc_bq.py \
    -- \
        --input_green=gs://dtc_data_lake_de-zoomcamp-nytaxi/pq/green/2020/*/ \
        --input_yellow=gs://dtc_data_lake_de-zoomcamp-nytaxi/pq/yellow/2020/*/ \
        --output=trips_data_all.report-2020

        .config("spark.jars.packages", "com.google.cloud.spark:spark-3.5-bigquery:0.37.0")

        gcloud dataproc jobs submit pyspark \
    --cluster=dezoomcamp2024-cluster1 \
    --region=us-east1 \
    gs://dezoomcamp2024_project/code/spark_sql_avi_script_dataproc_bq.py \
    -- \
        --input_green=gs://dtc_data_lake_de-zoomcamp-nytaxi/pq/green/2020/*/ \
        --input_yellow=gs://dtc_data_lake_de-zoomcamp-nytaxi/pq/yellow/2020/*/ \
        --output=trips_data_all.report-2020