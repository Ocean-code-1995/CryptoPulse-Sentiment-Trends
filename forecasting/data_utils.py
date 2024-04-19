#
import os
import pandas as pd
from pyspark.sql import SparkSession
from google.cloud import bigquery


def init_spark_session(app_name):
    return SparkSession.builder\
        .appName(app_name)\
        .config('spark.jars.packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.23.2') \
        .getOrCreate()

def read_data(spark, data_dir):
    return spark.read.parquet(data_dir)

def prepare_dataframe(df):
    df = df.toPandas()
    df = df.sort_values('Close_Time', ascending=True)
    return pd.DataFrame(
        {
            'ds': pd.to_datetime(
                df['Close_Time']
            ),  # Ensure 'Open Time' is in datetime format
            'y': df['Close'],  # Use the 'Close' price as the value to predict
        }
    )
##

def read_from_bigquery(spark, input_table):
    return spark.read.format("bigquery").option("table", input_table).load()