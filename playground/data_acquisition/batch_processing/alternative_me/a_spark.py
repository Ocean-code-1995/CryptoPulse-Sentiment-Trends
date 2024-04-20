from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month, to_timestamp
import pandas as pd
#
def spark_process_write(data: pd.DataFrame, output_path: str) -> None:
    """
    Processes and writes a Pandas DataFrame to a specified path in Parquet format
    with partitioning by year and month.

    Parameters:
    - data: Pandas DataFrame to be processed.
    - output_path: The output path where the Parquet files will be saved.
    """
    # Initialize SparkSession
    spark = SparkSession.builder.appName("AlternativeMeBatchProcessing").getOrCreate()

    # Convert Pandas DataFrame to Spark DataFrame
    df = spark.createDataFrame(data)

    # Rename 'timestamp' to 'Date', convert 'Date' from 'dd-MM-yyyy' format to Spark date type,
    # cast 'value' to integer, and 'value_classification' to string. Drop 'time_until_update' if exists.
    df = (
        df.withColumnRenamed("timestamp", "Date")
          #.withColumn("Date", to_date(col("Date"), "dd-MM-yyyy"))
          .withColumn("Date", to_timestamp(col("Date"), "dd-MM-yyyy"))  # Casting to timestamp
          .withColumn("value", col("value").cast("integer"))
          .withColumn("value_classification", col("value_classification").cast("string"))
          .drop("time_until_update")
          .sort("Date")
    )

    # Add 'Year' and 'Month' columns for partitioning
    df = (
        df.withColumn("Year", year("Date"))
          .withColumn("Month", month("Date"))
    )

    # Save the DataFrame to the specified path with partitioning by 'Year' and 'Month'
    df.write.partitionBy("Year", "Month")\
            .mode('overwrite')\
            .parquet(output_path)
##
