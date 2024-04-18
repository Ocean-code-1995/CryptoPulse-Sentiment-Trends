import os
from pyspark.sql import SparkSession, DataFrame
import pandas as pd
from typing import Iterator
from CryptoBertSentiment import CryptoBertSentiment
import logging

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

def read_process_write(input_path: str, output_path: str) -> None:
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

# Run the data processing pipeline:
if __name__ == "__main__":
    # Define your input and output directories
    #------------------------------------------------------
    # Get the path to the home directory
    home_dir = os.path.expanduser('~')

    input_path = os.path.join(
        home_dir, 'Desktop', 'DataEng_Crypto_Sentiment', 'playground', 'data', 'raw', 'reddit'
    )
    output_path = os.path.join(
        home_dir, 'Desktop', 'DataEng_Crypto_Sentiment', 'playground', 'data', 'processed', 'reddit_sentiment'
    )

    # Process the data
    read_process_write(
        input_path  = input_path,
        output_path = output_path
    )
    logging.info(">>> Data processing pipeline completed, successfully! <<<")
