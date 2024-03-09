from polygon import WebSocketClient, RESTClient
from polygon.websocket.models import WebSocketMessage, Market
from typing import List
# from base_ws import BaseTest
# from mock_server import subs, port
# import asyncio
from polygon.websocket import EquityTrade
from configparser import ConfigParser
from typing import cast
from urllib3 import HTTPResponse
import json
import pandas as pd


config_object = ConfigParser()
config_object.read("config.ini")
DEBUG_PRINT = config_object['main']['DEBUG_PRINT']

# Grab TD configuration values.
polygon_api_key = config_object.get('main', 'POLYGON_API_KEY')
polygon_client = RESTClient(polygon_api_key)
POLYGON_TRADES_HISTORY_RESPONSE_LIMIT = 50000

ticker = "AAPL"

# List Aggregates (Bars)
aggs = []
for a in polygon_client.list_aggs(ticker=ticker, multiplier=1, timespan="minute", from_="2023-01-01", to="2023-06-13", limit=50000):
    aggs.append(a)

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


def grap_stocks():
    pass

def grap_crypto():
    pass

stock_tickers = get_ticker_list('stocks')
crypto_tickers = get_ticker_list('crypto')
pass

# If you want to subscribe to all tickers, place an asterisk in place of the symbol. ["T.*"]

ws_crypto = WebSocketClient(api_key=polygon_api_key, market=Market.Crypto)
ws_crypto.subscribe("XT.BTC-USD")

ws_stocks = WebSocketClient(api_key=polygon_api_key, market=Market.Stocks)
ws_stocks.subscribe("T.*")

def crypto_handle_msg(msg: List[WebSocketMessage]):
    for m in msg:
        print(m)

ws_crypto.run(handle_msg=crypto_handle_msg)


def stocks_handle_msg(msg: List[WebSocketMessage]):
    for m in msg:
        print(m)

ws_stocks.run(handle_msg=stocks_handle_msg)



# class WebSocketsTest(BaseTest):
#     async def test_conn(self):
#         c = WebSocketClient(
#             api_key="", feed=f"localhost:{port}", verbose=True, secure=False
#         )
#         self.expectResponse(
#             [
#                 EquityTrade(
#                     event_type="T",
#                     symbol="AAPL",
#                     exchange=10,
#                     id="5096",
#                     tape=3,
#                     price=161.87,
#                     size=300,
#                     conditions=[14, 41],
#                     timestamp=1651684192462,
#                     sequence_number=4009402,
#                 )
#             ]
#         )
#         c.subscribe("T.AAPL")
#
#         def binded(msg):
#             self.expectProcessor(msg)
#
#         asyncio.get_event_loop().create_task(c.connect(binded))
#         self.expectResponse(
#             [
#                 EquityTrade(
#                     event_type="T",
#                     symbol="AMZN",
#                     exchange=12,
#                     id="72815",
#                     tape=3,
#                     price=161.87,
#                     size=1,
#                     conditions=[14, 37, 41],
#                     timestamp=1651684192536,
#                     sequence_number=4009408,
#                 ),
#                 EquityTrade(
#                     event_type="T",
#                     symbol="AMZN",
#                     exchange=4,
#                     id="799",
#                     tape=3,
#                     price=161.87,
#                     size=100,
#                     conditions=None,
#                     timestamp=1651684192717,
#                     sequence_number=4009434,
#                 ),
#             ]
#         )
#         c.subscribe("T.AMZN")
#         self.assertEqual(subs, c.subs)
#         c.unsubscribe_all()
#         self.assertEqual(subs, set())
#         c.subscribe("T.*")
#         self.assertEqual(subs, c.subs)
#         c.unsubscribe("T.*")
#         self.assertEqual(subs, set())
#         await c.close()
