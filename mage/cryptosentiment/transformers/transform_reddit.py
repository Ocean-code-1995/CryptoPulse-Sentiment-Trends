from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, year, month
from pyspark.sql import functions as F

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
    spark = SparkSession.builder.appName("RedditDataBatchProcessing").getOrCreate()
    df    = spark.createDataFrame(data)

    # Ensure column expressions are correct for string matching
    df = df.withColumn(
        "Currency",
        when(
            (col('Title').rlike('(?i)bitcoin|btc')) | (col('Body').rlike('(?i)bitcoin|btc')), 'Bitcoin'
        ).when(
            (col('Title').rlike('(?i)ethereum|eth')) | (col('Body').rlike('(?i)ethereum|eth')), 'Ethereum'
        ).otherwise('Other')
    )
#

    # Deduplicate, sort, and cast types
    df = df.dropDuplicates(['Title'])
    df = df.sort(col("Date").asc())

    df = df.select(
        col("Title").cast("string"),
        col("Body").cast("string"),
        col("Score").cast("integer"),
        col("Total Comments").cast("integer"),
        col("Votes").cast("float"),
        col("Date").cast("timestamp"),
        col("Currency").cast("string")
    )
    # create Title + Body column
    df = df.withColumn(
        "Title_Body",
        F.when(F.col("Body").isNull(), F.col("Title"))  # If Body is null, use Title
        .otherwise(F.concat(F.col("Title"), F.lit("\n"), F.col("Body")))  # Otherwise, concatenate Title, newline, and Body
    )

    # Extract year and month from the Open Time column for partitioning
    df = df.withColumn("Year", year(col("Date")))
    df = df.withColumn("Month", month(col("Date")))

    return df.toPandas()


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
