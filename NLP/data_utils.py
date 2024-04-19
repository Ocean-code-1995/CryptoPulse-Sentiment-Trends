import os
from pyspark.sql import SparkSession, DataFrame
import pandas as pd
from typing import Iterator
from CryptoBertSentiment import CryptoBertSentiment
import logging
from google.cloud import bigquery

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

os.environ['PYSPARK_PYTHON']        = '/Users/sebastianwefers/anaconda3/envs/cryptopulse_env/bin/python3.10'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/Users/sebastianwefers/anaconda3/envs/cryptopulse_env/bin/python3.10'

def process_partition(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    """
    Processes each partition of a Spark DataFrame through a sentiment analysis operation,
    yielding one partition at a time for memory-efficient, lazy evaluation. This generator
    function facilitates parallel processing in Spark's distributed environment, ensuring
    each partition is processed independently and only as needed.

    -> Utilizing 'yield' allows for handling large datasets with improved memory management by
    not requiring the entire dataset to be loaded into memory at once. It also enables
    effective error handling within each partition's processing, allowing the pipeline to
    continue even if a partition fails, by yielding the original partition data as a fallback.
    """
    for pandas_df in iterator:
        try:
            yield CryptoBertSentiment(data=pandas_df, col_name='Title_Body', batch_size=32)
        except Exception as e:
            logging.error(f"Error processing partition: {e}")
            yield pandas_df  # Optionally return the dataframe unchanged in case of error

def read_process_write_local(input_path: str, output_path: str) -> None:
    """
    Read data from input_path, process it using sentiment analysis, and write the result to output_path.
    """
    try:
        spark = SparkSession.builder.appName("SentimentAnalysis").getOrCreate()
        df    = spark.read.parquet(input_path)

        # Convert to Pandas DataFrame in partitions, apply sentiment analysis, and write back as Parquet:
        #  mapInPandas method efficiently leverages parallel processing and the power of Pandas for complex  Python operations on Spark DataFrame partitions, combining Spark's scalability with Pandas' functionality.
        processed_df = df.mapInPandas(process_partition, schema=df.schema)

        # Write the processed DataFrame back to Parquet in partitioned format
        processed_df.write.partitionBy("Year", "Month")\
                            .mode("overwrite")\
                                .parquet(output_path)

        logging.info(f"Sentiment analysis completed and data written to {output_path}.")

    except Exception as e:
        logging.error(f"Failed to complete the data processing pipeline: {e}")


def read_process_write_bigquery(input_table: str, output_table: str) -> None:
    try:
        # Setup Spark session to use BigQuery
        spark = SparkSession.builder \
            .appName("SentimentAnalysis") \
            .config('spark.jars.packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.23.2') \
            .getOrCreate()

        # Read from BigQuery
        df = spark.read \
            .format('bigquery') \
            .option('table', input_table) \
            .load()

        # Process data
        processed_df = df.mapInPandas(process_partition, schema=df.schema)

        # Write the processed DataFrame back to BigQuery
        processed_df.write \
            .format('bigquery') \
            .option('table', output_table) \
            .mode('overwrite') \
            .save()

        logging.info(f"Sentiment analysis completed and data written to {output_table}.")

    except Exception as e:
        logging.error(f"Failed to complete the data processing pipeline: {e}")
