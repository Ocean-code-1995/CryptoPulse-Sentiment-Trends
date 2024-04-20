##
import requests
import pandas as pd

def fetch_alternative_me(url: str, limit: int, data_format: str, date_format) -> pd.DataFrame:
    """ Fetch data from alternative.me API.
    """
    # define parameters for the request
    parameters = {
        "limit":       limit,
        "format":      data_format,
        "date_format": date_format
    }

    # make request
    response = requests.get(
        url    = url,
        params = parameters
    )

    # try, except block to check whether the request was successful
    try:
        response.raise_for_status()

    except requests.exceptions.HTTPError as e:
        print(f" -> An error occured: {e}")
        return None

    else:
        print(" -> Request was successful.")
        return pd.DataFrame(response.json()["data"])