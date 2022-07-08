import json
import requests


def get_company_ticker():
    url = "https://stock-market-data.p.rapidapi.com/market/index/s-and-p-six-hundred"

    headers = {
        "X-RapidAPI-Key": '3c708ba1b6msh81c7c2f280b7140p1c6fbajsnb891ccdfdb1d',
        "X-RapidAPI-Host": "stock-market-data.p.rapidapi.com"
    }
    response = requests.request("GET", url, headers=headers)
    res = json.loads(response.text)
    stocks = res['stocks']
    stocks = stocks[:2]
    return stocks
