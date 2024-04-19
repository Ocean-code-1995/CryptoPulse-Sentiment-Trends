import pandas as pd
import numpy as np
import streamlit as st
from typing import Union, Tuple
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pyspark.sql.functions as F
from pyspark.sql.types import TimestampType
import pyspark.sql.types as T

import os
from google.cloud import bigquery
from google.oauth2 import service_account

###
# Initialize BigQuery client function
def get_bigquery_client():
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if credentials_path is None:
        raise ValueError("GOOGLE_APPLICATION_CREDENTIALS environment variable is not set.")

    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    return bigquery.Client(credentials=credentials)

@st.cache_data(ttl=3600)
def load_and_process_BigQuery(query, project_id):
    client    = get_bigquery_client()
    query_job = client.query(query)  # Execute the query and fetch the results

    rows = query_job.result()
    data = [ dict(row.items()) for row in rows ]
    return pd.DataFrame(data)

# Usage of the modified function
def load_data(project_id: str):
    queries = {
        "reddit":     f"SELECT * FROM `{project_id}.cryptopulse_dataset.reddit_table`",
        "binance":    f"SELECT * FROM `{project_id}.cryptopulse_dataset.binance_table`",
        "fearngreed": f"SELECT * FROM `{project_id}.cryptopulse_dataset.fear_and_greed_table`",
        "forecast":   f"SELECT * FROM `{project_id}.cryptopulse_dataset.forecast_table`"
    }

    return {
        key: load_and_process_BigQuery(query, project_id)
        for key, query in queries.items()
    }

#@st.cache_data(ttl=3600, allow_output_mutation=True)  # Cache for 1 hour, adjust as needed
#def load_and_process_BigQuery(query, project_id):
    # Initialize BigQuery client using the credentials

#    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

#    if credentials_path is None:
#        raise ValueError("GOOGLE_APPLICATION_CREDENTIALS environment variable is not set.")

#    credentials = service_account.Credentials.from_service_account_file(credentials_path)
#    client      = bigquery.Client(project=project_id, credentials=credentials)

    # Execute the query and fetch the results
#    query_job = client.query(query)

    # Convert the query results to a DataFrame
#    rows = query_job.result()
#    data = [dict(row.items()) for row in rows]

#    return pd.DataFrame(data)



#@st.cache(ttl=3600, allow_output_mutation=True)  # Cache for 1 hour, adjust as needed
#def load_and_process_BigQuery1(query, project_id):
#    # Connect to Google BigQuery and run the query
#    return (
#        pd.read_gbq(query, project_id=project_id, progress_bar_type='console')
#        .dropna()
#    )


#from pyspark.sql import SparkSession
#import pyspark.sql.functions as F
#from pyspark.sql.types import TimestampType
import pandas as pd

#def load_partitioned_data(spark: SparkSession, path1: str, path2: str = None, path3: str = None, path4: str = None) -> tuple:
#    def to_pandas(df):
#        if df is not None:
#            # Print schema to understand the data types
#            df.printSchema()

            # Check and handle timestamp conversions
#            for field in df.schema.fields:
#                if isinstance(field.dataType, TimestampType):
#                    print(f"Casting {field.name} to timestamp")
#                    df = df.withColumn(field.name, F.col(field.name).cast("timestamp"))

            # Now attempt to convert to Pandas DataFrame
#            try:
#                pdf = df.toPandas()
#            except Exception as e:
#                print(f"Error converting to Pandas: {e}")
#                return None#
#
            # Ensure pandas datetime types are correct
#            for col_name in pdf.select_dtypes(include=['datetime64']).columns:
#                pdf[col_name] = pd.to_datetime(pdf[col_name], errors='coerce', exact=True, unit='ns')

#            return pdf

 #       return None

#    data1 = spark.read.parquet(path1) if path1 else None
#    data2 = spark.read.parquet(path2) if path2 else None
#    data3 = spark.read.parquet(path3) if path3 else None
#    data4 = spark.read.parquet(path4) if path4 else None#
#
#    pandas_data1 = to_pandas(data1)
#    pandas_data2 = to_pandas(data2)
#    pandas_data3 = to_pandas(data3)
#    pandas_data4 = to_pandas(data4)##

#    return pandas_data1, pandas_data2, pandas_data3, pandas_data4





def aggregate_by_date(df:pd.DataFrame, freq:str, col_name:str, date_col:str) -> pd.DataFrame:
    """
    Aggregates the specified column of the DataFrame by the given frequency.

    Parameters:
    - df: pd.DataFrame with a date/time column.
    - freq: str, frequency for aggregation. Accepts 'D' for day, 'M' for month, and 'Y' for year.
    - col_name: str, the name of the column to aggregate.
    - date_col: str, the name of the date/time column.

    Returns:
    - Aggregated DataFrame.
    """
    df = df.copy()
    if 'Date' in df.columns:
        df[date_col] = pd.to_datetime(df[date_col])
        # Set the date/time column as the index
        df.set_index(date_col, inplace=True)
    else:
        st.error("Date column not found in the DataFrame")
    # Resample the data by the given frequency and aggregate the specified column
    df_agg = df.resample(freq).agg({col_name: "mean"})

    # Drop missing values and reset the index
    df_agg = df_agg.dropna().reset_index()

    return df_agg


def merge_corr_dfs(
    df1:pd.DataFrame, df2:pd.DataFrame,
    date_col1:str, date_col2:str,
    col1:str, col2:str
    ) -> Tuple[pd.DataFrame, Union[float, None]]:
    """
    Merges two DataFrames based on the specified date columns and calculates the correlation between them.

    Parameters:
    - df1, df2: DataFrames to merge and calculate correlation.
    - date_col1, date_col2: Date columns in df1 and df2 to use for merging.
    - col1, col2: Columns in df1 and df2 to use for correlation calculation.

    Returns:
    - Merged DataFrame with correlation value.
    """
    df1, df2 = df1.copy(), df2.copy()
    # ensure both have the same date range and format
    df1[date_col1] = pd.to_datetime(df1[date_col1]).dt.normalize()
    df2[date_col2] = pd.to_datetime(df2[date_col2]).dt.normalize()

    # Merge the DataFrames based on the specified date columns
    merged_df = pd.merge(df1, df2, left_on=date_col1, right_on=date_col2, how="inner")

     # Focus only on the numeric columns intended for correlation
    if col1 in merged_df.columns and col2 in merged_df.columns:
        # Calculate the correlation between the two specified columns
        corr_matrix = merged_df[[col1, col2]].corr()
        corr_value = corr_matrix.loc[col1, col2]
    else:
        corr_value = None  # or handle the case where columns aren't available

    return merged_df, corr_value


def merge_and_correlate(df1, df2, date_col1, date_col2, cols1, col2):
    df1, df2 = df1.copy(), df2.copy()
    df1[date_col1] = pd.to_datetime(df1[date_col1]).dt.normalize()
    df2[date_col2] = pd.to_datetime(df2[date_col2]).dt.normalize()

    merged_df = pd.merge(df1, df2, left_on=date_col1, right_on=date_col2, how="inner")
    correlations = {}
    for col in cols1:
        if col in merged_df.columns:
            correlation = merged_df[[col, col2]].dropna().corr().iloc[0, 1]
            correlations[col] = correlation
    return correlations


