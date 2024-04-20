##
from a_fetch import fetch_alternative_me
from a_spark import spark_process_write

from dotenv import load_dotenv
import os

# set the python version
os.environ['PYSPARK_PYTHON']        = '/Users/sebastianwefers/anaconda3/envs/cryptopulse_env/bin/python3.10'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/Users/sebastianwefers/anaconda3/envs/cryptopulse_env/bin/python3.10'


def main() -> None:
    """
    Main function to fetch data from alternative.me, process it with Spark,
    and save the output in Parquet format with partitioning.
    """
    # 1) Define the parameters
    #------------------------------------------------------
    url         = "https://api.alternative.me/fng/"
    limit       =  360 + 104 # 360 days + 104 days (1 year + 3 months of 2024)
    date_format = "world"
    data_format = "json"

    # Define the output path
    output_path = os.path.join(
            os.path.expanduser('~'),
            'Desktop',"DataEng_Crypto_Sentiment", 'playground', 'data', 'raw', 'alternative_me'
    )

    # 2) Fetch data from Alternative.me
    #------------------------------------------------------
    data = fetch_alternative_me(url, limit, data_format, date_format)

    # 3) Process and save data using Spark
    #------------------------------------------------------
    if data is not None:
        spark_process_write(data, output_path)
    else:
        print("Data fetch failed, skipping data processing.")


# Run the main function
if __name__ == "__main__":
    main()
    print(">>> Batch processing completed! <<<")