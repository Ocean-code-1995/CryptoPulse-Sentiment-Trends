import os
import data_utils
import train_model
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession

#!!!!!!!!!!!!!!!! >>> NOTE: python3.8 for prophet ;) <<< !!!!!!!!!!!!!!!!
os.environ['PYSPARK_PYTHON']        = '/Users/sebastianwefers/anaconda3/envs/prophet_env/bin/python3.8'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/Users/sebastianwefers/anaconda3/envs/prophet_env/bin/python3.8'
#
def main():
    app_name      = "BinanceForecasting"
    data_dir      = os.path.join(
        os.path.expanduser("~"),
        'Desktop', "DataEng_Crypto_Sentiment", 'playground', 'data', 'raw', 'binance'
    )

    spark         = data_utils.init_spark_session(app_name)
    df            = data_utils.read_data(spark, data_dir)
    prophet_df    = data_utils.prepare_dataframe(df)
    train, test   = train_model.train_test_split(prophet_df)
    best_params   = train_model.optimize_hyperparameters(train)
    N_DAYS_FUTURE = 3
    forecast      = train_model.create_forecast(prophet_df, best_params, test, N_DAYS_FUTURE)


    # Merge historical and forecast data
    merged_df = prophet_df.merge(
        forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']],
        on='ds',
        how='outer'
    )

    # Write to file
    output_path = os.path.join(
        os.path.expanduser("~"),
        'Desktop', "DataEng_Crypto_Sentiment", 'playground',
        'data', 'processed', 'binance_forecast'
    )

    # write as partitioned parquet
    merged_df['Year'] = merged_df['ds'].dt.year
    merged_df['Month'] = merged_df['ds'].dt.month
    # SPARK DF
    merged_df = spark.createDataFrame(merged_df)
    merged_df.write.partitionBy("Year", "Month")\
                            .mode("overwrite")\
                                .parquet(output_path)


if __name__ == "__main__":
    main()