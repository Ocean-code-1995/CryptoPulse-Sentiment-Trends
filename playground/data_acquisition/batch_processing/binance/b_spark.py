from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, year, month
import pandas as pd
##
def spark_process_write(data: pd.DataFrame, output_path: str) -> None:
    """
    Processes and saves Binance data using Spark.
    Performs all necessary data transformations, including date conversions and type casting.
    """
    spark = SparkSession.builder.appName("BinanceDataBatchProcessing").getOrCreate()
    df = spark.createDataFrame(data)

    # Convert timestamps to datetime and cast data types
    df = df.select(
        from_unixtime(col("Open Time") / 1000).alias("Open Time").cast("timestamp"), # timestamp before
        col("Open").cast("float"),
        col("High").cast("float"),
        col("Low").cast("float"),
        col("Close").cast("float"),
        col("Volume").cast("float"),
        from_unixtime(col("Close Time") / 1000).alias("Close Time").cast("timestamp"),
        col("Quote Asset Volume").cast("float"),
        col("Number of Trades").cast("integer"),
        col("Taker Buy Base Asset Volume").cast("float"),
        col("Taker Buy Quote Asset Volume").cast("float")
    ).drop("Ignore")

    # Extract year and month from the Open Time column for partitioning
    df = df.withColumn("Year", year(col("Open Time")))
    df = df.withColumn("Month", month(col("Open Time")))

    # Save the DataFrame to the specified path
    df.write.partitionBy("Year", "Month")\
        .mode('overwrite')\
        .parquet(output_path)


