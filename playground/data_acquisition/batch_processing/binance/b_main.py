#
from b_fetch import fetch_binance_data
from b_spark import spark_process_write

from binance import Client
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

# set the python version
os.environ['PYSPARK_PYTHON']        = '/Users/sebastianwefers/anaconda3/envs/cryptopulse_env/bin/python3.10'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/Users/sebastianwefers/anaconda3/envs/cryptopulse_env/bin/python3.10'

def main() -> None:
    # 1) Define the parameters
    #------------------------------------------------------
    # Get the path to the home directory
    home_dir = os.path.expanduser('~')
    # Construct the path to the .env_vars file
    dotenv_path = os.path.join(home_dir, "Desktop", "DataEng_Crypto_Sentiment", '.env_vars')
    # Load the environment variables
    try:
        load_dotenv(dotenv_path=dotenv_path)
    except Exception:
        print("Error loading environment variables.")
    else:
        print("Environment variables loaded successfully.")

    # Define the Binance API key and secret key
    api_key     = os.getenv('BINANCE_APIKEY')
    secret_key  = os.getenv('BINANCE_SECRET')

    # Define the parameters for fetching Binance data
    days_in_past = 360 + 104 # 1 year + 3 months
    symbol       = 'BTCUSDT'
    interval     = Client.KLINE_INTERVAL_1DAY
    start_str    = ( datetime.now() - timedelta(days=days_in_past) ).strftime('%d %b %Y')

    # Define the output path
    output_path = os.path.join(
            os.path.expanduser('~'),
            'Desktop',"DataEng_Crypto_Sentiment", 'playground', 'data', 'raw', 'binance'
    )

    # 2) Fetch data from Binance
    #------------------------------------------------------
    data = fetch_binance_data(
        api_key    = api_key,
        secret_key = secret_key,
        symbol     = symbol,
        interval   = interval,
        start_str  = start_str
    )

    # 3) Process and save data using Spark
    #------------------------------------------------------
    if data is not None:
        spark_process_write(data, output_path)
    else:
        print("Data fetch failed, skipping data processing.")

#
# Run the main function
if __name__ == "__main__":
    main()
    print(">>> Data acquisition completed successfully. <<<")
