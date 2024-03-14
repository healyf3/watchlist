from configparser import ConfigParser

import polygon.exceptions
from finvizfinance.quote import finvizfinance
from polygon import RESTClient as plygRESTC
from typing import cast
from urllib3 import HTTPResponse
import json
import pandas as pd
pd.options.mode.copy_on_write = True
import time

config_object = ConfigParser()
config_object.read("config.ini")
DEBUG_PRINT = config_object['main']['DEBUG_PRINT']
LOG_PRINT = config_object['main']['LOG_PRINT']

# Grab TD configuration values.
polygon_api_key = config_object.get('main', 'POLYGON_API_KEY')
polygon_client = plygRESTC(polygon_api_key)
POLYGON_TRADES_HISTORY_RESPONSE_LIMIT = 50000


def dbg_print(val):
    if DEBUG_PRINT == 'True':
        print(val)

def log_print(val):
    if LOG_PRINT == 'True':
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


def get_ticker_df(market_type, price_lower_bound=None, price_upper_bound=None, market_cap_limit=None, excluded_types=None):

    include_otc = False
    if market_type == 'stocks':
        include_otc = True

    tickers = cast(
        HTTPResponse,
        polygon_client.get_snapshot_all(market_type=market_type, include_otc=include_otc, raw=True),
    )

    ddict = json.loads(tickers.data.decode("utf-8"))
    df = pd.DataFrame(ddict['tickers'])

    # Extract last price from dictionary and put it in a column
    if market_type == 'stocks':
        last_quote_frame = df['lastQuote'].apply(pd.Series)
        df = pd.concat([df, last_quote_frame], axis=1).drop(columns=['lastQuote', 'S','p','s','t'])
    prev_day_frame = df['prevDay'].apply(pd.Series)
    # Simplify DataFrame
    df = pd.concat([df, prev_day_frame], axis=1).drop(columns=['updated', 'lastTrade', 'min', 'day', 'prevDay','o','h','l','v','vw'])

    if price_upper_bound is not None:
        price_upper_bound_mask = df['P'] <= price_upper_bound
        df = df[price_upper_bound_mask]
    if price_lower_bound is not None:
        price_lower_bound_mask = df['P'] >= price_lower_bound
        df = df[price_lower_bound_mask]


    # df['market cap'] = ''
    # for index, row in df.iterrows():
    #     print(df.loc[index, 'ticker'])
    #     try:
    #         ticker_details = cast(
    #             HTTPResponse,
    #             polygon_client.get_ticker_details(ticker=row['ticker']),
    #         )
    #         df.loc[index, 'market cap'] = ticker_details.market_cap
    #     except polygon.exceptions.BadResponse as e:
    #         df.loc[index, 'market cap'] = "bad response"
    #         print(e)

   # mask = df['market cap'] <= market_cap_limit
   # df = df[~mask]

    return df

    #return tickers_df['ticker'].sort_values().tolist()


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