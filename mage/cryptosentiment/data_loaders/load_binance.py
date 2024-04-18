import io
import pandas as pd
import requests
from datetime import datetime
from binance import Client
from binance.exceptions import BinanceAPIException, BinanceRequestException
from mage_ai.data_preparation.shared.secrets import get_secret_value

from datetime import datetime, timedelta

# Importing decorators if they're not already defined
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


def fetch_binance_data(
    api_key:    str,
    secret_key: str,
    symbol:     str,
    interval:   str,
    start_str:  str
) -> pd.DataFrame:
    """
    Fetches data from Binance based on the given parameters.
    """
    # Establish connection to the Binance API
    client = Client(
        api_key,
        secret_key
    )
    # try except block to evaluate whether api key and secret key are valid and connection is established
    try:
        client.get_account()
    except (BinanceAPIException, BinanceRequestException) as e:
        raise ValueError(f"API Error: {e.status_code} - {e.message}") from e
    else:
        print("API connection established successfully.")

    # Fetch historical klines
    klines = client.get_historical_klines(
        symbol    = symbol,
        interval  = interval,
        start_str = start_str,
    )
    return pd.DataFrame(
        data    = klines,
        columns = [
            "Open_Time",
            "Open",
            "High",
            "Low",
            "Close",
            "Volume",
            "Close_Time",
            "Quote_Asset_Volume",
            "Number_of_Trades",
            "Taker_Buy_Base_Asset_Volume",
            "Taker_Buy_Quote_Asset_Volume",
            "Ignore",
        ],
    )


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Load historical daily data for a symbol from Binance API.
    """
    # 1) Define the parameters for the data loader
    # ------------------------------------------------
    #days_in_past = 360 + 104 # 1 year + 3 months
    symbol       = kwargs['symbol']
    interval     = Client.KLINE_INTERVAL_1DAY
    start_str    = datetime.strptime(kwargs['start_date'], "%Y-%m-%d").strftime('%d %b %Y') 

    # #!! used before without trigger and incremental as defined per condition below

     # Determine if it's the initial load or an incremental load
    #if 'current_date' in kwargs:
        # Incremental load: Fetch data from the past day
    #    current_date = datetime.strptime(kwargs['current_date'], "%Y-%m-%d")
    #    start_date   = current_date - timedelta(days=1)
    #else:
        # Initial load: Fetch data starting from fixed start date
    #    start_date = datetime.strptime("2023-7-1", "%Y-%m-%d")

    #start_str = start_date.strftime('%d %b %Y')

    # 2) Fetch data from Binance API and return it
    # ------------------------------------------------
    return fetch_binance_data(
        api_key    = get_secret_value('bi_key'),
        secret_key = get_secret_value('bi_secret'),
        symbol     = symbol,
        interval   = interval,
        start_str  = start_str,
    )


@test
def test_output(output, *args) -> None:
    """
    Test code for testing the output of the data loader.
    """
    assert output is not None, 'The output is undefined'
    assert not output.empty, 'The output dataframe is empty'
    assert 'Open' in output.columns, 'DataFrame should contain "Open" column'
