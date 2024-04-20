# spark_processor.py #
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, year, month
import pandas as pd
from pyspark.sql import functions as F


def spark_process_write(data:pd.DataFrame, output_path: str) -> None:
    """
    Process the data and save it to the specified path.
    """
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
        col("ID").cast("string"),
        col("Author").cast("string"),
        col("Title").cast("string"),
        col("Body").cast("string"),
        col("Score").cast("integer"),
        col("Total Comments").cast("integer"),
        col("Votes").cast("float"),
        col("URL").cast("string"),
        col("Date").cast("timestamp"),
        col("Currency").cast("string")
    )
    # Assuming 'df' is your DataFrame
    df = df.withColumn(
        "Title_Body",
        F.when(F.col("Body").isNull(), F.col("Title"))  # If Body is null, use Title
        .otherwise(F.concat(F.col("Title"), F.lit("\n"), F.col("Body")))  # Otherwise, concatenate Title, newline, and Body
    )

    # Extract year and month from the Open Time column for partitioning
    df = df.withColumn("Year", year(col("Date")))
    df = df.withColumn("Month", month(col("Date")))

    # Saving the DataFrame to the specified path
    df.write.partitionBy("Year", "Month")\
        .mode('overwrite')\
            .parquet(output_path)

