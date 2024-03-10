import time

from polygon import WebSocketClient, RESTClient
from polygon.websocket.models import WebSocketMessage, Market
from typing import List
from configparser import ConfigParser
from typing import cast
from urllib3 import HTTPResponse
import json
import pandas as pd
import threading
import util
from util import polygon_api_key
import sinch_sms

config_object = ConfigParser()
config_object.read("config.ini")

#stock_tickers = get_ticker_list('stocks')
#crypto_tickers = get_ticker_list('crypto')
stock_trades_df = pd.DataFrame(columns=['ticker', 'trade count'])
stock_minute_agg_df = pd.DataFrame(columns=['ticker', 'price', 'accumulated volume', 'vwap', 'trades in minute', 'max volume', 'did max volume close red'])
crypto_trades_df = pd.DataFrame(columns=['ticker', 'trade count'])
crypto_minute_agg_df = pd.DataFrame(columns=['ticker', 'price', 'accumulated volume', 'vwap', 'trades in minute', 'max volume', 'did minute close red'])
def ws_handle_msg(msg: List[WebSocketMessage]):
    for m in msg:
        util.dbg_print(m)
        if m.event_type == 'T':
            if not (stock_trades_df == m.pair).any().any():
                stock_trades_df.loc[len(stock_trades_df.index)] = [m.pair, 1]
            else:
                index = stock_trades_df.index[crypto_trades_df['ticker'] == m.pair]
                stock_trades_df.loc[index, 'trade count'] += 1
        elif m.event_type == 'AM':
            # sinch_sms.send_sms_alert(m.pair, m.close,"fundamentals", "cat")
            pass
        elif m.event_type == 'XT':
            if not (crypto_trades_df == m.pair).any().any():
                crypto_trades_df.loc[len(crypto_trades_df.index)] = [m.pair, 1]
            else:
                index = crypto_trades_df.index[crypto_trades_df['ticker'] == m.pair]
                crypto_trades_df.loc[index, 'trade count'] += 1
        elif m.event_type == 'XA':
            if not (crypto_minute_agg_df == m.pair).any().any():
                minute_agg_df_index = len(crypto_minute_agg_df.index)
                crypto_minute_agg_df.loc[len(crypto_minute_agg_df.index)] = [m.pair, m.close, m.volume, m.vwap, 0, m.volume, m.close < m.open]
                if not (crypto_trades_df == m.pair).any().any():
                    crypto_minute_agg_df.loc[minute_agg_df_index, 'trades in minute'] = 0
                else:
                    trades_df_index = crypto_trades_df.index[crypto_trades_df['ticker'] == m.pair]
                    crypto_minute_agg_df.loc[minute_agg_df_index, 'trades in minute'] = crypto_trades_df.loc[trades_df_index, 'trade count'].values[0] - crypto_minute_agg_df.loc[minute_agg_df_index, 'trades in minute']
            else:
                if not (crypto_trades_df == m.pair).any().any():
                    crypto_minute_agg_df.loc[minute_agg_df_index, 'trades in minute'] = 0
                else:
                    trades_df_index = crypto_trades_df.index[crypto_trades_df['ticker'] == m.pair]
                    crypto_minute_agg_df.loc[minute_agg_df_index, 'trades in minute'] = crypto_trades_df.loc[trades_df_index, 'trade count'].values[0] - crypto_minute_agg_df.loc[minute_agg_df_index, 'trades in minute']

                minute_agg_df_index = crypto_minute_agg_df.index[crypto_minute_agg_df['ticker'] == m.pair]
                crypto_minute_agg_df.loc[minute_agg_df_index, 'accumulated volume'] += m.volume
                if m.volume > crypto_minute_agg_df.loc[minute_agg_df_index, 'max volume']:
                    crypto_minute_agg_df.loc[minute_agg_df_index, 'max volume'] = m.volume
                crypto_minute_agg_df.loc[minute_agg_df_index, 'did minute close red'] = m.close < m.open


        # sinch_sms.send_sms_alert(m.pair, m.close,"fundamentals", "cat")


def run_crypto_socket():
    ws_crypto = WebSocketClient(api_key=polygon_api_key, market=Market.Crypto)
    ws_crypto.subscribe("XA.*", "XT.*")

    ws_crypto.run(ws_handle_msg)


def run_crypto_socket2():
    ws_crypto_2 = WebSocketClient(api_key=polygon_api_key, market=Market.Crypto)
    ws_crypto_2.subscribe("XA.LTC-USD")

    ws_crypto_2.run(ws_handle_msg)

def run_stock_socket():
    ws_stocks = WebSocketClient(api_key=polygon_api_key, market=Market.Stocks)
    ws_stocks.subscribe("T.*")
    ws_stocks.run(ws_handle_msg)


def main():

    thread_crypto = threading.Thread(target=run_crypto_socket)
    thread_stock = threading.Thread(target=run_stock_socket)
    #thread_crypto2 = threading.Thread(target=run_crypto_socket2)

    #run_crypto_socket()

    thread_crypto.start()
    thread_stock.start()
    #thread_crypto2.start()
    thread_crypto.join()
    thread_stock.join()
    #thread_crypto2.join()

main()