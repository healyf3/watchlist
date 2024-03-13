import time

from polygon import WebSocketClient, RESTClient
from polygon.websocket.models import WebSocketMessage, Market
from typing import List
from configparser import ConfigParser
from typing import cast
from urllib3 import HTTPResponse
import json
import pandas as pd
pd.options.mode.copy_on_write = True
import threading
import util
from util import polygon_api_key
import sinch_sms

config_object = ConfigParser()
config_object.read("config.ini")

watchlist_msg_queue = []

MARKET_CAP_LIMIT = 800000000
PRICE_UPPER_BOUND= 50
PRICE_LOWER_BOUND= 0.08
TRADE_MINUTE_LIQUIDITY_LOWER_BOUND = 20
stock_tickers_df = util.get_ticker_df('stocks', price_lower_bound=PRICE_LOWER_BOUND, price_upper_bound=PRICE_UPPER_BOUND, market_cap_limit=MARKET_CAP_LIMIT)
crypto_tickers_df = util.get_ticker_df('crypto', market_cap_limit=MARKET_CAP_LIMIT)

stock_trades_df = pd.DataFrame(columns=['ticker', 'trade count'])
stock_minute_agg_df = pd.DataFrame(columns=['ticker', 'price', 'accumulated volume', 'v*p cumsum', 'vwap', 'below vwap signal', 'trades in minute', 'max volume', 'did max volume close red',
                                            'prev close', '18% gain signal', '30% gain signal', '50% gain signal', '70% gain signal', '100% gain signal', '200% gain signal'])
crypto_trades_df = pd.DataFrame(columns=['ticker', 'trade count'])
crypto_minute_agg_df = pd.DataFrame(columns=['ticker', 'price', 'accumulated volume', 'vwap', 'trades in minute', 'max volume', 'did max volume close red',
                                             'prev close', '18% gain signal', '30% gain signal', '50% gain signal', '70% gain signal', '100% gain signal', '200% gain signal'])

def service_enqueued_alerts():
    global watchlist_msg_queue
    for alert in watchlist_msg_queue:
        sinch_sms.send_sms_alert(alert['category'], alert['symbol'], alert['price'])

    watchlist_msg_queue = []

def ws_handle_msg(msg: List[WebSocketMessage]):
    for m in msg:
        util.dbg_print(m)
        if m.event_type == 'T':
            if not (stock_trades_df == m.symbol).any().any():
                stock_trades_df.loc[len(stock_trades_df.index)] = [m.symbol, 1]
            else:
                index = stock_trades_df.index[stock_trades_df['ticker'] == m.symbol]
                stock_trades_df.loc[index, 'trade count'] += 1
        elif m.event_type == 'AM':
            v_times_p = m.volume*((m.close+m.high+m.low)/3)
            if not (stock_minute_agg_df == m.symbol).any().any():
                minute_agg_df_index = len(stock_minute_agg_df.index)
                stock_minute_agg_df.loc[minute_agg_df_index] = [m.symbol, m.close, m.volume, v_times_p, m.vwap, False, 0, m.volume, m.close < m.open, float(0),
                                                                False, False, False, False, False, False]
                # sometimes there is no previous close due to halts or new IPO
                #if len(stock_tickers_df.loc[stock_tickers_df['ticker'] == m.symbol]['prevDay'].values) == 1:
                stock_minute_agg_df.loc[minute_agg_df_index, 'prev close'] = stock_tickers_df.loc[stock_tickers_df['ticker'] == m.symbol]['c'].values[0]
                #tickers_df.loc[tickers_df['ticker'] == 'AAPL']['prevDay'].values[0]['c']
                if not (stock_trades_df == m.symbol).any().any():
                    stock_minute_agg_df.loc[minute_agg_df_index, 'trades in minute'] = 0
                else:
                    trades_df_index = stock_trades_df.index[stock_trades_df['ticker'] == m.symbol].values[0]
                    stock_minute_agg_df.loc[minute_agg_df_index, 'trades in minute'] = stock_trades_df.loc[trades_df_index, 'trade count'] - stock_minute_agg_df.loc[minute_agg_df_index, 'trades in minute']
            else:
                minute_agg_df_index = stock_minute_agg_df.index[stock_minute_agg_df['ticker'] == m.symbol]
                if not (stock_trades_df == m.symbol).any().any():
                    stock_minute_agg_df.loc[minute_agg_df_index, 'trades in minute'] = 0
                else:
                    trades_df_index = stock_trades_df.index[stock_trades_df['ticker'] == m.symbol].values[0]
                    stock_minute_agg_df.loc[minute_agg_df_index, 'trades in minute'] = stock_trades_df.loc[trades_df_index, 'trade count'] - stock_minute_agg_df.loc[minute_agg_df_index, 'trades in minute']

                stock_minute_agg_df.loc[minute_agg_df_index, 'accumulated volume'] += m.volume
                stock_minute_agg_df.loc[minute_agg_df_index, 'v*p cumsum'] += v_times_p
                stock_minute_agg_df.loc[minute_agg_df_index, 'vwap'] = stock_minute_agg_df.loc[minute_agg_df_index, 'v*p cumsum'] / stock_minute_agg_df.loc[minute_agg_df_index, 'accumulated volume']
                if m.volume > stock_minute_agg_df.loc[minute_agg_df_index, 'max volume'].values[0]:
                    stock_minute_agg_df.loc[minute_agg_df_index, 'max volume'] = m.volume
                    stock_minute_agg_df.loc[minute_agg_df_index, 'did max volume close red'] = m.close < m.open


                if stock_minute_agg_df.loc[minute_agg_df_index, 'trades in minute'].values[0] > TRADE_MINUTE_LIQUIDITY_LOWER_BOUND:
                    if stock_minute_agg_df.loc[minute_agg_df_index, 'did max volume close red'].values[0]:
                        watchlist_msg_queue.append({'category': 'max red candle alert', 'symbol': m.symbol, 'price': m.close})
                    daily_gain = (m.close - stock_minute_agg_df.loc[minute_agg_df_index, 'prev close'].values[0]) / stock_minute_agg_df.loc[minute_agg_df_index, 'prev close'].values[0]
                    if daily_gain >= 0.18 and stock_minute_agg_df.loc[minute_agg_df_index, '18% gain signal'].values[0] == False:
                        watchlist_msg_queue.append({'category': '18% gain alert', 'symbol': m.symbol, 'price': m.close})
                        stock_minute_agg_df.loc[minute_agg_df_index, '18% gain signal'] = True
                    if daily_gain >= 0.30 and stock_minute_agg_df.loc[minute_agg_df_index, '30% gain signal'].values[0] == False:
                        watchlist_msg_queue.append({'category': '30% gain alert', 'symbol': m.symbol, 'price': m.close})
                        stock_minute_agg_df.loc[minute_agg_df_index, '30% gain signal'] = True
                    if daily_gain >= 0.50 and stock_minute_agg_df.loc[minute_agg_df_index, '50% gain signal'].values[0] == False:
                        watchlist_msg_queue.append({'category': '50% gain alert', 'symbol': m.symbol, 'price': m.close})
                        stock_minute_agg_df.loc[minute_agg_df_index, '50% gain signal'] = True
                    if daily_gain >= 0.70 and stock_minute_agg_df.loc[minute_agg_df_index, '70% gain signal'].values[0] == False:
                        watchlist_msg_queue.append({'category': '70% gain alert', 'symbol': m.symbol, 'price': m.close})
                        stock_minute_agg_df.loc[minute_agg_df_index, '70% gain signal'] = True
                    if daily_gain >= 1.00 and stock_minute_agg_df.loc[minute_agg_df_index, '100% gain signal'].values[0] == False:
                        watchlist_msg_queue.append({'category': '100% gain alert', 'symbol': m.symbol, 'price': m.close})
                        stock_minute_agg_df.loc[minute_agg_df_index, '100% gain signal'] = True
                    if daily_gain >= 2.00 and stock_minute_agg_df.loc[minute_agg_df_index, '200% gain signal'].values[0] == False:
                        watchlist_msg_queue.append({'category': '200% gain alert', 'symbol': m.symbol, 'price': m.close})
                        stock_minute_agg_df.loc[minute_agg_df_index, '200% gain signal'] = True

                if stock_minute_agg_df.loc[minute_agg_df_index, '18% gain signal'].values[0]:
                    #if m.close < stock_minute_agg_df.loc[minute_agg_df_index, 'vwap']:
                    if not stock_minute_agg_df.loc[minute_agg_df_index, 'below vwap signal'].values[0]:
                        if m.close < m.aggregate_vwap:
                            watchlist_msg_queue.append({'category': 'below vwap alert', 'symbol': m.symbol, 'price': m.close})
                            stock_minute_agg_df.loc[minute_agg_df_index, 'below vwap signal'] = True
                        else:
                            stock_minute_agg_df.loc[minute_agg_df_index, 'below vwap signal'] = False


                   # stock_minute_agg_df.loc[minute_agg_df_index, '30% gain signal'] == True || \
                   # stock_minute_agg_df.loc[minute_agg_df_index, '50% gain signal'] == True || \
                   # stock_minute_agg_df.loc[minute_agg_df_index, '70% gain signal'] == True || \
                   # stock_minute_agg_df.loc[minute_agg_df_index, '100% gain signal'] == True || \
                   # stock_minute_agg_df.loc[minute_agg_df_index, '200% gain signal'] == True:

        elif m.event_type == 'XT':
            if not (crypto_trades_df == m.pair).any().any():
                crypto_trades_df.loc[len(crypto_trades_df.index)] = [m.pair, 1]
            else:
                index = crypto_trades_df.index[crypto_trades_df['ticker'] == m.pair]
                crypto_trades_df.loc[index, 'trade count'] += 1
        elif m.event_type == 'XA':
            if not (crypto_minute_agg_df == m.pair).any().any():
                minute_agg_df_index = len(crypto_minute_agg_df.index)
                crypto_minute_agg_df.loc[minute_agg_df_index] = [m.pair, m.close, m.volume, m.vwap, 0, m.volume, m.close < m.open,
                                                                 crypto_tickers_df.loc[crypto_tickers_df['ticker'] == 'X:'+ m.pair.replace('-','')]['prevDay'].values[0]['c'],
                                                                 False, False, False, False, False, False]
                if not (crypto_trades_df == m.pair).any().any():
                    crypto_minute_agg_df.loc[minute_agg_df_index, 'trades in minute'] = 0
                else:
                    trades_df_index = crypto_trades_df.index[crypto_trades_df['ticker'] == m.pair].values[0]
                    crypto_minute_agg_df.loc[minute_agg_df_index, 'trades in minute'] = crypto_trades_df.loc[trades_df_index, 'trade count'] - crypto_minute_agg_df.loc[minute_agg_df_index, 'trades in minute']
            else:
                minute_agg_df_index = crypto_minute_agg_df.index[crypto_minute_agg_df['ticker'] == m.pair]
                if not (crypto_trades_df == m.pair).any().any():
                    crypto_minute_agg_df.loc[minute_agg_df_index, 'trades in minute'] = 0
                else:
                    trades_df_index = crypto_trades_df.index[crypto_trades_df['ticker'] == m.pair].values[0]
                    crypto_minute_agg_df.loc[minute_agg_df_index, 'trades in minute'] = crypto_trades_df.loc[trades_df_index, 'trade count'] - crypto_minute_agg_df.loc[minute_agg_df_index, 'trades in minute']

                crypto_minute_agg_df.loc[minute_agg_df_index, 'accumulated volume'] += m.volume
                if m.volume > crypto_minute_agg_df.loc[minute_agg_df_index, 'max volume'].values[0]:
                    crypto_minute_agg_df.loc[minute_agg_df_index, 'max volume'] = m.volume
                    crypto_minute_agg_df.loc[minute_agg_df_index, 'did max volume close red'] = m.close < m.open
                    if crypto_minute_agg_df.loc[minute_agg_df_index, 'did max volume close red']:
                        watchlist_msg_queue.append('max red candle alert', m.pair, m.close)
        else:
            print("event is " + m.event_type)

    service_enqueued_alerts()



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
    #ws_stocks.subscribe("T.*", "AM.*")
    tickers_list = stock_tickers_df['ticker'].tolist()
    subscription_list = ["AM." + s for s in tickers_list]
    subscription_list += ["T." + s for s in tickers_list]
    # place in subscription format
    ss_str = str(subscription_list)
    ss_str = ss_str.replace("[","")
    ss_str = ss_str.replace("]","")
    ws_stocks.subscribe(ss_str)
    ws_stocks.run(ws_handle_msg)


def main():

    #thread_crypto = threading.Thread(target=run_crypto_socket)
    thread_stock = threading.Thread(target=run_stock_socket)
    #thread_crypto2 = threading.Thread(target=run_crypto_socket2)

    #run_crypto_socket()

    #thread_crypto.start()
    thread_stock.start()
    #thread_crypto2.start()
    #thread_crypto.join()
    thread_stock.join()
    #thread_crypto2.join()

main()