from configparser import ConfigParser
from finvizfinance.quote import finvizfinance
from polygon import RESTClient as plygRESTC
from typing import cast
from urllib3 import HTTPResponse
import json
import pandas as pd
import time

config_object = ConfigParser()
config_object.read("config.ini")
DEBUG_PRINT = bool(config_object['main']['DEBUG_PRINT'])

# Grab TD configuration values.
polygon_api_key = config_object.get('main', 'POLYGON_API_KEY')
polygon_client = plygRESTC(polygon_api_key)
POLYGON_TRADES_HISTORY_RESPONSE_LIMIT = 50000


def dbg_print(val):
    if DEBUG_PRINT == True:
        print(val)


def convert_stock_info_string_to_float(info):
    """
    :type info: 'xxxM' or 'xxxB' (M: 1 Million and B: 1 Billion)
    """
    if info[-1] == 'M':
        info = float(info[0:-1]) * 1000000
        return info
    elif info[-1] == 'B':
        info = float(info[0:-1]) * 1000000000
        return info
    else:
        print('float data has suffix other than M or B.')


def get_ticker_list(market_type):
    include_otc = False
    if market_type == 'stocks':
        include_otc = True

    tickers = cast(
        HTTPResponse,
        polygon_client.get_snapshot_all(market_type=market_type, include_otc=include_otc, raw=True),
    )

    ddict = json.loads(tickers.data.decode("utf-8"))
    tickers_df = pd.DataFrame(ddict['tickers'])

    return tickers_df['ticker'].sort_values().tolist()


def grab_finviz_fundamentals(ticker):
    stock_dict = dict()

    stock_dict['Float'] = 'na'
    stock_dict['Market Cap'] = 'na'
    stock_dict['Sector'] = 'na'
    stock_dict['Industry'] = 'na'
    stock_dict['Exchange'] = 'na'
    stock_dict['Inst Own'] = 'na'
    stock_dict['Short Float'] = 'na'

    try:
        stock = finvizfinance(ticker)
    except:
        print("error grabbing data for ticker: " + ticker)
        return stock_dict

    dbg_print('grab fundamentals for ' + ticker)
    fundamentals = stock.ticker_fundament()

    dbg_print(ticker)

    try:
        stock_dict['Float'] = convert_stock_info_string_to_float(fundamentals['Shs Float'])
    except:
        print('Shs Float not available for ' + ticker)
    try:
        stock_dict['Market Cap'] = convert_stock_info_string_to_float(fundamentals['Market Cap'])
    except:
        print('Market Cap not available for ' + ticker)
    try:
        stock_dict['Sector'] = fundamentals['Sector']
    except:
        print('Sector not available for ' + ticker)
    try:
        stock_dict['Industry'] = fundamentals['Industry']
    except:
        print('Industry not available for ' + ticker)
    try:
        stock_dict['Exchange'] = fundamentals['Exchange']
    except:
        print('Exchange not available for ' + ticker)
    try:
        stock_dict['Inst Own'] = fundamentals['Inst Own']
    except:
        print('Inst Own not available for ' + ticker)
    try:
        stock_dict['Short Float'] = fundamentals['Short Float']
    except:
        print('Short Float not available for ' + ticker)

    return stock_dict