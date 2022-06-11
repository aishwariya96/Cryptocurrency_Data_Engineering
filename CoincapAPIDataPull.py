from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
import datetime
from datetime import date, timedelta
import pandas as pd
from constants import data_pull_limit

url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
parameters = {
    'start': '1',
    'limit': data_pull_limit,
    'convert': 'EUR'
}

headers = {
    'Accepts': 'application/json',
    'X-CMC_PRO_API_KEY': '3e0fa2b4-9f65-40ed-b5a3-d45e61ac0fa6'
}

session = Session()
session.headers.update(headers)

try:
    response = session.get(url, params=parameters)
    data = json.loads(response.text)
    today = date.today()
    yesterday = str(today - timedelta(days=1))
    yesterday_datetime = datetime.datetime.strptime(yesterday, '%Y-%m-%d')
    # there are two keys 'status' and 'data'
    for entry in data["data"]:
        symbol = entry["symbol"]

        # gets only the date at string
        date_added_str = entry["date_added"][:10]
        # converting to datetime for comparison
        date_added = datetime.datetime.strptime(date_added_str, '%Y-%m-%d')
        if yesterday_datetime < date_added:
            print(symbol + ": " + date_added_str)
        else:
            pass
    print(data)

except (ConnectionError, Timeout, TooManyRedirects) as e:
    print(e)

# Preprocessing data
coincap_df = pd.DataFrame(data["data"])
coincap_df = pd.concat([coincap_df.drop(['quote'], axis=1), pd.json_normalize(coincap_df['quote'])], axis=1)
coincap_df = coincap_df.drop(columns=['tags', 'platform'])
coincap_json = coincap_df.to_json(orient='records')

with open('crypto_data.json', 'w') as f:
    json.dump(coincap_json, f)

