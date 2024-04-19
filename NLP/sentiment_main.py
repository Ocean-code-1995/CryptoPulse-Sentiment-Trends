import os
from pyspark.sql import SparkSession, DataFrame
import pandas as pd
from typing import Iterator
from CryptoBertSentiment import CryptoBertSentiment
import logging

from data_utils import (
    process_partition,
    read_process_write_local,
    read_process_write_bigquery
)
###
# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

os.environ['PYSPARK_PYTHON']        = '/Users/sebastianwefers/anaconda3/envs/cryptopulse_env/bin/python3.10'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/Users/sebastianwefers/anaconda3/envs/cryptopulse_env/bin/python3.10'


# Run the data processing pipeline:
if __name__ == "__main__":
    # Define your input and output directories
    #------------------------------------------------------

    # a) Get the path to the home directory and read from local files
    #------------------------------------------------------
    #home_dir = os.path.expanduser('~')

    #input_path = os.path.join(
    #    home_dir, 'Desktop', 'DataEng_Crypto_Sentiment', 'playground', 'data', 'raw', 'reddit'
    #)
    #output_path = os.path.join(
    #    home_dir, 'Desktop', 'DataEng_Crypto_Sentiment', 'playground', 'data', 'processed', 'reddit_sentiment'
    #)

    # Process the data
    #read_process_write(
    #    input_path  = input_path,
    #    output_path = output_path
    #)

    # b) Read & write from/to GCS
    #------------------------------------------------------
    project_id = os.getenv("PROJECT_ID")
    # Specify your BigQuery table names
    input_table  = f'{project_id}.cryptopulse.reddit_table'
    output_table = f'{project_id}.cryptopulse.reddit_table'

    # Process the data
    read_process_write_bigquery(
        input_table  = input_table,
        output_table = output_table
    )
    logging.info(">>> Data processing pipeline completed, successfully! <<<")