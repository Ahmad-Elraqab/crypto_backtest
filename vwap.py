from ast import And
from asyncio.windows_events import NULL
from cmath import nan
from datetime import datetime
import math
import threading
import uuid
from binance.client import Client
from binance.streams import ThreadedWebsocketManager
import numpy as np
from pandas.core.frame import DataFrame
from client import send_message
from config import API_KEY, API_SECRET, exchange_pairs
import pandas as pd
import time
import os
import errno
import concurrent.futures

FILE_NAME = 'RSI-15M-stream-300'
coin_list = {}
kilne_tracker = {}
client = Client(api_key=API_KEY, api_secret=API_SECRET)
excel_df = DataFrame(columns=['id', 'symbol', 'type', 'interval', 'amount',
                              'startDate', 'endDate', 'buy', 'sell', 'growth/drop', 'closed', 'status', '15', '30', '45', '60', 'vwap20'])
ordersList = {}

INTERVAL = '15m'
H_HISTORY = Client.KLINE_INTERVAL_15MINUTE
PART = '-'
temp = False
tempTime = None


class Order:
    def __init__(self, id, type, symbol, interval, buyPrice, sellPrice, amount, startDate, volume, rsi, status):

        self.id = id
        self.type = type
        self.symbol = symbol
        self.interval = interval
        self.buyPrice = buyPrice
        self.sellPrice = sellPrice
        self.amount = amount
        self.startDate = startDate
        self.rate = None
        self.endDate = startDate
        self.isSold = False
        self.isHold = False
        self.hold = buyPrice
        self.high = buyPrice
        self.low = buyPrice
        self.volume = volume
        self.rsi = rsi
        self.status = status


def readHistory(i):

    try:
        global c_df

        # klines = client.get_historical_klines(
        #     symbol=i, interval=H_HISTORY, start_str="14 Feb, 2022", end_str="19 Feb, 2022")
        klines = client.get_historical_klines(
            symbol=i, interval=H_HISTORY, start_str="3 days ago")

        data = pd.DataFrame(klines)

        data[0] = pd.to_datetime(data[0], unit='ms')

        data.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'IGNORE', 'Quote_Volume',
                        'Trades_Count', 'BUY_VOL', 'BUY_VOL_VAL', 'x']

        data = data.drop(columns=['IGNORE',
                                  'Trades_Count', 'BUY_VOL', 'BUY_VOL_VAL', 'x'])

        data['Close'] = pd.to_numeric(
            data['Close'], errors='coerce')

        coin_list[i] = {'s': i, 'active': False,
                        'buy': False, 'type': None, 'set-buy': False}

        kilne_tracker[i] = data

        calcVWAP(symbol=i, msg=data, inte=20)
        calcVWAP(symbol=i, msg=data, inte=48)
        calcVWAP(symbol=i, msg=data, inte=84)

    except Exception as e:
        pass


def calcVWAP(symbol, msg, inte):

    high = pd.to_numeric(kilne_tracker[symbol]['High'])
    low = pd.to_numeric(kilne_tracker[symbol]['Low'])
    close = pd.to_numeric(kilne_tracker[symbol]['Close'])
    volume = pd.to_numeric(kilne_tracker[symbol]['Volume'])

    value1 = ((high + low + close) / 3 * volume).rolling(inte).sum()

    value2 = volume.rolling(inte).sum()

    kilne_tracker[symbol]['DIF_' + str(inte)] = value1 / value2


def buy(index, row, symbol):

    # try:
    global coin_list
    status = coin_list[symbol]['active']
    buy_ = coin_list[symbol]['buy']
    vwap20 = row.DIF_20
    vwap48 = row.DIF_48
    vwap84 = row.DIF_84

    list = np.min([vwap20, vwap48, vwap84])
    close = row.Close
    open = row.Open
    volume = row.Volume

    global excel_df

    if math.isnan(vwap84) == True or math.isnan(vwap48) == True or math.isnan(vwap20) == True:
        # print(vwap20)
        pass

    else:
        limit = pd.to_numeric(kilne_tracker[symbol].iloc[index, kilne_tracker[symbol].columns.get_loc(
            coin_list[symbol]['type'])])

        COIN = (close - kilne_tracker[symbol].iloc[index-1,
                kilne_tracker[symbol].columns.get_loc('Close')]) / close * 100
        if buy_ == False and status == True and pd.to_numeric(close) < pd.to_numeric(limit) and pd.to_numeric(close) > pd.to_numeric(list):

            print('buy')
            coin_list[symbol]['buy'] = True

            order = Order(
                id=uuid.uuid1(),
                type='rsi',
                symbol=symbol,
                interval=INTERVAL,
                buyPrice=close,
                sellPrice=close + (close * 0.05),
                amount=500,
                startDate=row.Date,
                volume=volume,
                rsi=0,
                status=status
            )
            ordersList['list'].append(order)

            msg = {
                'id': order.id,
                'symbol': order.symbol,
                'type': order.type,
                'interval': order.interval,
                'amount': order.amount,
                'startDate': order.startDate,
                'endDate': order.endDate,
                'buy': order.buyPrice,
                'sell': order.sellPrice,
                'closed': order.isSold,
                'growth/drop': order.rate,
                'status': status,
                'high': order.high,
                'low': order.low,
                'Volume': order.volume,
                'RSI': order.rsi,
                'coin': (close - kilne_tracker[symbol].iloc[index-1, kilne_tracker[symbol].columns.get_loc('Close')]) / close * 100,
                'BTC': (pd.to_numeric(kilne_tracker['BTCUSDT'].iloc[index, kilne_tracker['BTCUSDT'].columns.get_loc('Close')]) - pd.to_numeric(kilne_tracker['BTCUSDT'].iloc[index-1, kilne_tracker['BTCUSDT'].columns.get_loc('Close')])) / pd.to_numeric(kilne_tracker['BTCUSDT'].iloc[index, kilne_tracker['BTCUSDT'].columns.get_loc('Close')]) * 100,
                'V-BTC': kilne_tracker['BTCUSDT'].iloc[index-1, kilne_tracker['BTCUSDT'].columns.get_loc('Volume')],

            }
            excel_df = excel_df.append(msg, ignore_index=True)

            excel_df.to_csv(f'results/data@'+FILE_NAME+'.csv')

    # except Exception as e:
    #     pass


def checkSell(rate, order, price, time):

    order.sellPrice = price
    order.endDate = time
    order.rate = rate
    order.sellZscore = 0

    if price > order.high:
        order.high = price

    elif price < order.low:
        order.low = price

    global excel_df

    difference = (order.endDate - order.startDate)
    total_seconds = difference.total_seconds()

    hours = divmod(total_seconds, 60)[0]

    if rate > 0.5 or (rate <= -3.0) or hours >= 45.0:

        coin_list[order.symbol]['active'] = False
        coin_list[order.symbol]['buy'] = False

        order.isSold = True
        ordersList[order.symbol]['date'] = datetime.now()

        excel_df.loc[excel_df['id'] == order.id, 'sell'] = order.sellPrice
        excel_df.loc[excel_df['id'] == order.id, 'endDate'] = order.endDate
        excel_df.loc[excel_df['id'] == order.id, 'closed'] = order.isSold
        excel_df.loc[excel_df['id'] == order.id, 'buy'] = order.buyPrice
        excel_df.loc[excel_df['id'] == order.id, 'growth/drop'] = order.rate

    excel_df.to_csv(f'results/data@'+FILE_NAME+'.csv')


def sell(s, time, price):

    list = ordersList['list']
    p = float(price)

    for i in list:

        if i.isSold == False and i.symbol == s:

            rate = ((float(price) - float(i.buyPrice)) /
                    float(i.buyPrice)) * 100

            checkSell(rate, i, p, time)


def checkTouch(symbol, row):

    vwap20 = row.DIF_20
    vwap48 = row.DIF_48
    vwap84 = row.DIF_84

    status = coin_list[symbol]['active']

    if pd.to_numeric(row.Close) >= pd.to_numeric(vwap20) and status == False:

        coin_list[symbol]['active'] = True
        coin_list[symbol]['type'] = 'DIF_20'

    elif pd.to_numeric(row.Close) >= pd.to_numeric(vwap48) and status == False:

        coin_list[symbol]['active'] = True
        coin_list[symbol]['type'] = 'DIF_48'

    elif pd.to_numeric(row.Close) >= pd.to_numeric(vwap84) and status == False:

        coin_list[symbol]['active'] = True
        coin_list[symbol]['type'] = 'DIF_84'


def handle_socket():

    for key, value in kilne_tracker.items():

        for index, row in value.iterrows():

            checkTouch(symbol=key, row=row)
            buy(index, row, symbol=key)
            sell(price=row.Close, s=key, time=row.Date)

    print(excel_df)


def init():
    ordersList['list'] = []
    for pair in exchange_pairs:
        ordersList[pair] = {}


init()


t1 = time.perf_counter()

with concurrent.futures.ThreadPoolExecutor() as executor:

    executor.map(readHistory, exchange_pairs)

t2 = time.perf_counter()

print(f'Finished in {t2 - t1} seconds')

handle_socket()
