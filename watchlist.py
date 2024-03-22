#!/home/ec2-user/.local/share/virtualenvs/watchlist-dO_9O89D/bin/python3

import sys
import time
import datetime
from polygon import WebSocketClient, RESTClient
from polygon.websocket.models import WebSocketMessage, Market
from typing import List, Dict
from configparser import ConfigParser
from urllib3 import HTTPResponse
import json
import pandas as pd
import threading
import util
from util import polygon_api_key
import sinch_sms
import time
import os


config_object = ConfigParser()
base_path = os.path.dirname(os.path.realpath(__file__))
config_read_path = config_object.read(os.path.join(base_path, "config.ini"))
config_object.read(config_read_path)
ALERTS_ENABLED = config_object['main']['ALERTS_ENABLED']

alerts_msg_queue = []

MARKET_CAP_LIMIT = 800000000
PRICE_UPPER_BOUND = 17
PRICE_LOWER_BOUND = 0.12
TRADE_MINUTE_LIQUIDITY_LOWER_BOUND = 250
DOLLAR_VOLUME_LOWER_LIMIT = 5000000

BELOW_VWAP_ALERT_LIMIT = 2

print("STARTING WATCHLIST")

stock_tickers_df = util.get_ticker_df('stocks', price_lower_bound=PRICE_LOWER_BOUND, price_upper_bound=PRICE_UPPER_BOUND, market_cap_limit=MARKET_CAP_LIMIT)
crypto_tickers_df = util.get_ticker_df('crypto', market_cap_limit=MARKET_CAP_LIMIT)

stock_trades_df = pd.DataFrame(columns=['ticker', 'trade count'])
stock_minute_agg_df = pd.DataFrame(columns=['ticker', 'price', 'accumulated volume', 'v*p cumsum', 'vwap', 'below vwap signal', 'trades in minute', 'max volume', 'did max volume close red',
                                            'prev close', '18% gain signal', '30% gain signal', '50% gain signal', '70% gain signal', '100% gain signal', '200% gain signal'])
crypto_trades_df = pd.DataFrame(columns=['ticker', 'trade count'])
crypto_minute_agg_df = pd.DataFrame(columns=['ticker', 'price', 'accumulated volume', 'vwap', 'trades in minute', 'max volume', 'did max volume close red',
                                             'prev close', '18% gain signal', '30% gain signal', '50% gain signal', '70% gain signal', '100% gain signal', '200% gain signal'])

def service_enqueued_alerts():
    global alerts_msg_queue
    while(1):
        if ALERTS_ENABLED == 'True':
            for alert in alerts_msg_queue:
                try:
                    sinch_sms.send_sms_alert(alert['category'], alert['symbol'], alert['price'])
                except:
                    e = sys.exc_info()[0]
                    print('sms error occurred: ' + str(e))

        alerts_msg_queue = []
        time.sleep(5)

trades_map: Dict[str, int] = {}
trades_current_minute_map: Dict[str, int] = {}
trades_aggregated_last_minute_map: Dict[str, int] = {}
volume_map: Dict[str, int] = {}
vwap_map: Dict[str, int] = {}
max_volume_map: Dict[str, int] = {}
max_volume_red_candle_map: Dict[str, int] = {}
price_map: Dict[str, int] = {}
prev_close_map: Dict[str, int] = stock_tickers_df.set_index('ticker').filter(['c']).to_dict()['c']
high_time_map: Dict[str, int] = {}
high_map: Dict[str, int] = {}
gain_18_p_signal_map: Dict[str, bool] = {}
gain_30_p_signal_map: Dict[str, bool] = {}
gain_50_p_signal_map: Dict[str, bool] = {}
gain_70_p_signal_map: Dict[str, bool] = {}
gain_100_p_signal_map: Dict[str, bool] = {}
gain_200_p_signal_map: Dict[str, bool] = {}
below_vwap_signal_map: Dict[str, bool] = {}
below_vwap_signal_count_map: Dict[str, int] = {}
todays_open: Dict[str, bool] = {}

## Max red candle requirements:
# 1. must be biggest volume minute of the day
# 2. must be within 10 minutes of the high of day time
# 3. must be atleast an 18% gainer

def ws_handle_msg(msg: List[WebSocketMessage]):
#     while True:
        #print(ws_queue.unfinished_tasks)
        #msg = ws_queue.get()
        for m in msg:
            util.log_print(m)
            is_max_volume = False

            if m.event_type == 'T':
                trades_map[m.symbol] = trades_map.get(m.symbol, 0) + 1
            elif m.event_type == 'AM':
                if todays_open.get(m.symbol, 0) == 0:
                    todays_open[m.symbol] = m.open
                price_map[m.symbol] = m.close
                vwap_map[m.symbol] = m.aggregate_vwap
                trades_current_minute_map[m.symbol] = trades_map.get(m.symbol, 0) - trades_aggregated_last_minute_map.get(m.symbol, 0)
                trades_aggregated_last_minute_map[m.symbol] = trades_map.get(m.symbol, 0)
                if max_volume_map.get(m.symbol, 0) < m.volume:
                    max_volume_map[m.symbol] = m.volume
                    is_max_volume = True
                if high_map.get(m.symbol, 0) < m.close:
                    high_map[m.symbol] = m.close
                    high_time_map[m.symbol] = datetime.datetime.fromtimestamp(m.end_timestamp/1000)


                if trades_current_minute_map[m.symbol] > TRADE_MINUTE_LIQUIDITY_LOWER_BOUND:
                    daily_gain = (m.close - prev_close_map[m.symbol]) / prev_close_map[m.symbol]
                    if 0.18 <= daily_gain < 0.30 and gain_18_p_signal_map.get(m.symbol, 0) != True:
                        util.dbg_print(str(daily_gain*100) + "% gain for " + m.symbol)
                        alerts_msg_queue.append({'category': '18% gain alert', 'symbol': m.symbol, 'price': m.close})
                        gain_18_p_signal_map[m.symbol] = True
                    if 0.30 <= daily_gain < 0.50 and gain_30_p_signal_map.get(m.symbol, 0) != True:
                        util.dbg_print(str(daily_gain*100) + "% gain for " + m.symbol)
                        alerts_msg_queue.append({'category': '30% gain alert', 'symbol': m.symbol, 'price': m.close})
                        gain_18_p_signal_map[m.symbol] = True
                        gain_30_p_signal_map[m.symbol] = True
                    if 0.50 <= daily_gain < 0.70 and gain_50_p_signal_map.get(m.symbol, 0) != True:
                        util.dbg_print(str(daily_gain*100) + "% gain for " + m.symbol)
                        alerts_msg_queue.append({'category': '50% gain alert', 'symbol': m.symbol, 'price': m.close})
                        gain_18_p_signal_map[m.symbol] = True
                        gain_30_p_signal_map[m.symbol] = True
                        gain_50_p_signal_map[m.symbol] = True
                    if 0.70 <= daily_gain < 1.00 and gain_70_p_signal_map.get(m.symbol, 0) != True:
                        util.dbg_print(str(daily_gain*100) + "% gain for " + m.symbol)
                        alerts_msg_queue.append({'category': '70% gain alert', 'symbol': m.symbol, 'price': m.close})
                        gain_18_p_signal_map[m.symbol] = True
                        gain_30_p_signal_map[m.symbol] = True
                        gain_50_p_signal_map[m.symbol] = True
                        gain_70_p_signal_map[m.symbol] = True
                    if 1.00 <= daily_gain < 2.00 and gain_100_p_signal_map.get(m.symbol, 0) != True:
                        util.dbg_print(str(daily_gain*100) + "% gain for " + m.symbol)
                        alerts_msg_queue.append({'category': '100% gain alert', 'symbol': m.symbol, 'price': m.close})
                        gain_18_p_signal_map[m.symbol] = True
                        gain_30_p_signal_map[m.symbol] = True
                        gain_50_p_signal_map[m.symbol] = True
                        gain_70_p_signal_map[m.symbol] = True
                        gain_100_p_signal_map[m.symbol] = True
                    if 2.00 <= daily_gain and gain_200_p_signal_map.get(m.symbol, 0) != True:
                        util.dbg_print(str(daily_gain*100) + "% gain for " + m.symbol)
                        alerts_msg_queue.append({'category': '200% gain alert', 'symbol': m.symbol, 'price': m.close})
                        gain_18_p_signal_map[m.symbol] = True
                        gain_30_p_signal_map[m.symbol] = True
                        gain_50_p_signal_map[m.symbol] = True
                        gain_70_p_signal_map[m.symbol] = True
                        gain_100_p_signal_map[m.symbol] = True
                        gain_200_p_signal_map[m.symbol] = True

                    if gain_18_p_signal_map.get(m.symbol, 0) == True or gain_30_p_signal_map.get(m.symbol, 0) == True  or gain_50_p_signal_map.get(m.symbol, 0) == True or \
                        gain_70_p_signal_map.get(m.symbol, 0) == True or gain_100_p_signal_map.get(m.symbol, 0) == True or gain_200_p_signal_map.get(m.symbol, 0) == True:
                        if m.close < m.open and is_max_volume and ((datetime.datetime.now() - high_time_map[m.symbol]).seconds < 60*10) and (((m.close - todays_open[m.symbol])/todays_open[m.symbol]) >= 0.18) :
                            util.dbg_print("max red candle for " + m.symbol)
                            alerts_msg_queue.append({'category': 'max red candle alert', 'symbol': m.symbol, 'price': m.close})
                        if m.close < m.aggregate_vwap:
                            if below_vwap_signal_map.get(m.symbol, 0) == False and (below_vwap_signal_count_map.get(m.symbol, 0) <= BELOW_VWAP_ALERT_LIMIT):
                                util.dbg_print("below vwap for " + m.symbol)
                                alerts_msg_queue.append({'category': 'below vwap alert', 'symbol': m.symbol, 'price': m.close})
                                below_vwap_signal_map[m.symbol] = True
                                below_vwap_signal_count_map[m.symbol] = below_vwap_signal_count_map.get(m.symbol, 0) + 1
                        else:
                            below_vwap_signal_map[m.symbol] = False


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
                            alerts_msg_queue.append('max red candle alert', m.pair, m.close)
            else:
                print("event is " + m.event_type)

        #ws_queue.task_done()


def run_crypto_socket():
    ws_crypto = WebSocketClient(api_key=polygon_api_key, market=Market.Crypto)
    ws_crypto.subscribe("XA.*", "XT.*")

    ws_crypto.run(ws_handle_msg)
    print("why did the socket end")


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
    #subscription_list = ["T.AAPL"]
    # place in subscription format
    ss_str = str(subscription_list)
    ss_str = ss_str.replace("[","")
    ss_str = ss_str.replace("]","")
    ws_stocks.subscribe(ss_str)
    ws_stocks.run(ws_handle_msg)

def market_time():
    while True:
        if datetime.datetime.now().hour >= 20:
            print('close websocket for the day, exit program, and start again tomorrow')
            os._exit(1)
        time.sleep(60)


def watchlist():

    #thread_crypto = threading.Thread(target=run_crypto_socket)
    thread_stock = threading.Thread(target=run_stock_socket)
    thread_market_time = threading.Thread(target=market_time)
    thread_service_enqueued_alerts = threading.Thread(target=service_enqueued_alerts)
    # thread_ws_queue = threading.Thread(target=ws_queue_handler)
    ##thread_crypto2 = threading.Thread(target=run_crypto_socket2)
    #thread_hello = threading.Thread(target=hello)

    #run_crypto_socket()
    #thread_crypto.start()
    #thread_crypto2.start()
    #thread_crypto.join()
    #thread_crypto2.join()

    #print("start")
    #run_stock_socket()
    #print("finish")

    thread_stock.start()
    thread_market_time.start()
    thread_service_enqueued_alerts.start()

    thread_market_time.join()
    #thread_market_time.join()
    #thread_hello.start()
    #thread_ws_queue.start()
    #thread_stock.join()
    #thread_hello.join()
    #thread_ws_queue.join()
    #while(1):
    #    pass
#        print('hello main')
#        time.sleep(1)

if __name__ == '__main__':
    watchlist()
