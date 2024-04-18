#
import os
import pandas as pd
from pyspark.sql import SparkSession

def init_spark_session(app_name):
    return SparkSession.builder.appName(app_name).getOrCreate()

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
#