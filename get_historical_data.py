import json
import pandas as pd
import requests
from get_company_name import get_company_ticker


def get_historical_data():
    stocks = get_company_ticker()
    for i in range(len(stocks)):
        url = "https://stock-market-data.p.rapidapi.com/stock/historical-prices"
        querystring = {"ticker_symbol": str(stocks[i]), "years": "1", "format": "json"}
        headers = {
            "X-RapidAPI-Key": '3c708ba1b6msh81c7c2f280b7140p1c6fbajsnb891ccdfdb1d',
            "X-RapidAPI-Host": "stock-market-data.p.rapidapi.com"
        }
        response = requests.request("GET", url, headers=headers, params=querystring)
        res = json.loads(response.text)
        df = pd.DataFrame(res['historical prices'])
        df.to_csv('csv_database/' + str(stocks[i] + '.csv'))

get_historical_data()
