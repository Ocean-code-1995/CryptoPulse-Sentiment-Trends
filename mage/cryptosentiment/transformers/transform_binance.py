from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, year, month
import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    spark = SparkSession.builder \
        .appName("AlternativeMeBatchProcessing") \
        .getOrCreate()

    df = spark.createDataFrame(data)

    # Convert timestamps to datetime and cast data types
    df = df.select(
        from_unixtime(col("Open_Time") / 1000).alias("Open_Time").cast("timestamp"), # timestamp before
        col("Open").cast("float"),
        col("High").cast("float"),
        col("Low").cast("float"),
        col("Close").cast("float"),
        col("Volume").cast("float"),
        from_unixtime(col("Close_Time") / 1000).alias("Close_Time").cast("timestamp"),
        col("Quote_Asset_Volume").cast("float"),
        col("Number_of_Trades").cast("integer"),
        col("Taker_Buy_Base_Asset_Volume").cast("float"),
        col("Taker_Buy_Quote_Asset_Volume").cast("float")
    ).drop("Ignore")

    # Extract year and month from the Open Time column for partitioning
    df = df.withColumn("Year", year(col("Open Time")))
    df = df.withColumn("Month", month(col("Open Time")))

    return df.toPandas()


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
