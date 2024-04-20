# main.py
from r_fetch import fetch_subreddit_data
from r_spark import spark_process_write
from dotenv import load_dotenv
import os

# set the python version
os.environ['PYSPARK_PYTHON']        = '/Users/sebastianwefers/anaconda3/envs/cryptopulse_env/bin/python3.10'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/Users/sebastianwefers/anaconda3/envs/cryptopulse_env/bin/python3.10'



def main() -> None:
    # 1) authentication to Reddit API
    #------------------------------------------------------
    # Get the path to the home directory
    home_dir = os.path.expanduser('~')
    # Construct the path to the .env_vars file
    dotenv_path = os.path.join(home_dir, 'Desktop', "DataEng_Crypto_Sentiment", '.env_vars')
    # Load the environment variables
    try:
        load_dotenv(dotenv_path=dotenv_path)
    except Exception:
        print("Error loading environment variables.")
    else:
        print("Environment variables loaded successfully.")
#
    # Define the authentication parameters
    authentication  = {
        'REDDIT_CLIENT_ID':     os.getenv('REDDIT_CLIENT_ID'),
        'REDDIT_CLIENT_SECRET': os.getenv('REDDIT_CLIENT_SECRET'),
        'REDDIT_USER_AGENT':    os.getenv('REDDIT_USER_AGENT'),
        'REDDIT_USERNAME':      os.getenv('REDDIT_USERNAME'),
        'REDDIT_PASSWORD':      os.getenv('REDDIT_PASSWORD')
    }#
    # 2) Define the parameters
    #------------------------------------------------------
    subreddit_name      = 'CryptoCurrency'
    sort                = 'new'
    months_back         = 6
    requests_per_minute = 1000  # Be mindful of Reddit's rate limit.
    limit_per_request   = 100
    keywords_list       = ['bitcoin', 'btc']
    output_path         = os.path.join(home_dir, 'Desktop', "DataEng_Crypto_Sentiment", 'playground', 'data', 'raw', 'reddit')

    # 3) Fetch data from Reddit
    #------------------------------------------------------
    data = fetch_subreddit_data(
        authentication      = authentication,
        subreddit_name      = subreddit_name,
        sort                = sort,
        months_back         = months_back,
        requests_per_minute = requests_per_minute,
        limit_per_request   = limit_per_request,
        keywords_list       = keywords_list
    )

    # 4) Process and save data using Spark
    #------------------------------------------------------
    if data is not None:
        spark_process_write(data, output_path)
    else:
        print("Data fetch failed, skipping data processing.")


# Run the main function
if __name__ == "__main__":
    main()
    print(">>> Data acquisition completed successfully. <<<")
