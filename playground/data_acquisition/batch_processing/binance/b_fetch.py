#
from binance import Client
from binance.exceptions import BinanceAPIException, BinanceRequestException
import pandas as pd


def fetch_binance_data(
    api_key: str,
    secret_key: str,
    symbol: str,
    interval: str,
    start_str: str
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
            "Open Time",
            "Open",
            "High",
            "Low",
            "Close",
            "Volume",
            "Close Time",
            "Quote Asset Volume",
            "Number of Trades",
            "Taker Buy Base Asset Volume",
            "Taker Buy Quote Asset Volume",
            "Ignore",
        ],
    )