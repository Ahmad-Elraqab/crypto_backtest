from os import close
import threading
import time
from binance.streams import ThreadedWebsocketManager
from models.node import Node
from numpy import e, sqrt
import numpy as np
from pandas.core.frame import DataFrame
from pandas.core.tools.numeric import to_numeric
from config import API_KEY, API_SECRET, exchange_pairs
import concurrent.futures
from binance.client import AsyncClient, Client
from client import send_message
import pandas as pd
import matplotlib.pyplot as plt
import uuid

ordersList = {}
kilne_tracker = {}
client = Client(api_key=API_KEY, api_secret=API_SECRET)
excel_df = DataFrame(columns=['id', 'symbol', 'type', 'interval', 'amount',
                              'startDate', 'endDate', 'buy', 'sell', 'growth/drop', 'drop_count', 'total', 'closed', 'buy_zscore', 'sell_zscore', 'alert_zscore', 'alert_price', 'alert_date', 'high', 'low'])

coin_list = {}


class Order:
    def __init__(self, id, type, symbol, interval, buyPrice, sellPrice, amount, startDate, dropRate, buyZscore):

        self.id = id
        self.type = type
        self.symbol = symbol
        self.interval = interval
        self.buyPrice = buyPrice
        self.sellPrice = sellPrice
        self.amount = amount
        self.startDate = startDate
        self.dropRate = dropRate
        self.total = buyPrice
        self.rate = None
        self.endDate = None
        self.drop_count = 1
        self.isSold = False
        self.sellZscore = None
        self.buyZscore = buyZscore
        self.high = buyPrice
        self.low = buyPrice


def zScore(window, close, volume):

    mean = (volume*close).rolling(window=window).sum() / \
        volume.rolling(window=window).sum()

    vwapsd = sqrt(pow(close-mean, 2).rolling(window=window).mean())

    return (close-mean)/(vwapsd)


def setDatafFame(symbol):

    close = pd.to_numeric(kilne_tracker[symbol]['Close'])

    volume = pd.to_numeric(kilne_tracker[symbol]['Volume'])

    kilne_tracker[symbol]['48-zscore'] = zScore(
        window=48, close=close, volume=volume)

    kilne_tracker[symbol]['Close'] = pd.to_numeric(
        kilne_tracker[symbol]['Close'])


def readHistory(i):

    klines = client.get_historical_klines(
        # symbol=i, interval=Client.KLINE_INTERVAL_5MINUTE, start_str="10 days ago")
        symbol=i, interval=Client.KLINE_INTERVAL_5MINUTE, start_str="1 Oct, 2021", end_str="1 Nov, 2021")

    data = pd.DataFrame(klines)

    data[0] = pd.to_datetime(data[0], unit='ms')

    data.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'IGNORE', 'Quote_Volume',
                    'Trades_Count', 'BUY_VOL', 'BUY_VOL_VAL', 'x']

    data = data.drop(columns=['IGNORE',
                              'Trades_Count', 'BUY_VOL', 'BUY_VOL_VAL', 'x'])

    # data = data.set_index('Date')
    # coin_list[i] = {'s': i, 'price': 0,
    #                 'status': False, 'date': None, 'current': 0, 'zscore': 0, 'btc': 0, 'btc-v': 0}
    data['Close'] = pd.to_numeric(
        data['Close'], errors='coerce')
    data['Volume'] = pd.to_numeric(
        data['Volume'], errors='coerce')
    data['High'] = pd.to_numeric(
        data['High'], errors='coerce')
    data['Low'] = pd.to_numeric(
        data['Low'], errors='coerce')

    kilne_tracker[i] = data

    setDatafFame(i)


def sell(s, time, low, price, index):

    list = ordersList['list']
    p = float(price)

    # try:
    for i in list:

        if i.isSold == False and time != i.startDate and i.symbol == s:

            symbol = i.symbol

            rate = ((float(price) - float(i.buyPrice)) /
                    float(i.buyPrice)) * 100

            zscore = kilne_tracker[symbol].iloc[index,
                                                kilne_tracker[symbol].columns.get_loc('48-zscore')]

            difference = (time - i.startDate)
            total_seconds = difference.total_seconds()
            hours = divmod(total_seconds, 60)[0]

            if pd.to_numeric(price) > pd.to_numeric(i.high):
                i.high = price

            elif pd.to_numeric(low) < pd.to_numeric(i.low):
                i.low = price

            if rate >= 0.5 or zscore >= 2.0:
                # if zscore >= 2.0:
                # if zscore >= 2.0:

                i.isSold = True
                # coin_list[symbol]['status'] = False
                ordersList[symbol]['isBuy'] = False

                i.sellPrice = price
                i.endDate = time
                i.rate = rate
                i.sellZscore = kilne_tracker[symbol].iloc[index]['48-zscore']

                excel_df.loc[excel_df['id'] == i.id, 'sell'] = i.sellPrice
                excel_df.loc[excel_df['id'] == i.id, 'endDate'] = i.endDate
                excel_df.loc[excel_df['id'] == i.id, 'closed'] = i.isSold
                excel_df.loc[excel_df['id'] == i.id,
                             'drop_count'] = i.drop_count
                excel_df.loc[excel_df['id'] == i.id,
                             'sell_zscore'] = i.sellZscore
                excel_df.loc[excel_df['id'] ==
                             i.id, 'growth/drop'] = i.rate
                excel_df.loc[excel_df['id'] == i.id, 'high'] = i.high
                excel_df.loc[excel_df['id'] == i.id, 'low'] = i.low

                excel_df.to_csv(f'results/data@zscore-5m-test4.csv')


def buy(index, symbol, time, temp):

    try:
        zscore = kilne_tracker[symbol].iloc[index,
                                            kilne_tracker[symbol].columns.get_loc('48-zscore')]
        diff_ = (kilne_tracker['BTCUSDT'].iloc[index]['Close'] - kilne_tracker['BTCUSDT'].iloc[index -
                                                                                               temp]['Close']) / kilne_tracker['BTCUSDT'].iloc[index]['Close'] * 100
        diff_2 = (kilne_tracker[symbol].iloc[index]['Close'] - kilne_tracker[symbol].iloc[index -
                                                                                          temp]['Close']) / kilne_tracker[symbol].iloc[index]['Close'] * 100
        # if coin_list[symbol]['status'] == True and ordersList[symbol]['isBuy'] == False:
        # if ordersList[symbol]['isBuy'] == False and zscore <= -2.5 and diff_ >= 0:
        # list = [x for x in ordersList['list'] if x.isSold == False]

        if ordersList[symbol]['isBuy'] == False and zscore <= -2.5 and diff_ >= 0:

            ordersList[symbol]['isBuy'] = True

            order = Order(
                id=uuid.uuid1(),
                type='zscore',
                symbol=symbol,
                interval='15m',
                buyPrice=kilne_tracker[symbol].iloc[index]['Close'],
                sellPrice=kilne_tracker[symbol].iloc[index]['Close'] +
                (kilne_tracker[symbol].iloc[index]['Close'] * 0.05),
                amount=500,
                startDate=time,
                dropRate=5,
                buyZscore=zscore
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
                'drop_count': order.drop_count,
                'total': order.total,
                'closed': order.isSold,
                'growth/drop': order.rate,
                'buy_zscore': order.buyZscore,
                'sell_zscore': order.sellZscore,
                'high': order.high,
                'low': order.low,
                'BTC_Price': (kilne_tracker['BTCUSDT'].iloc[index]['Close'] - kilne_tracker['BTCUSDT'].iloc[index - 1]['Close']) / kilne_tracker['BTCUSDT'].iloc[index]['Close'] * 100,
                'BTC_Volume': (kilne_tracker['BTCUSDT'].iloc[index]['Volume'] - kilne_tracker['BTCUSDT'].iloc[index-1]['Volume']) / kilne_tracker['BTCUSDT'].iloc[index]['Volume'] * 100,
                'COIN_Price': (kilne_tracker[symbol].iloc[index]['Volume'] - kilne_tracker[symbol].iloc[index-1]['Volume']) / kilne_tracker[symbol].iloc[index]['Volume'] * 100,
                'COIN_Volume': (kilne_tracker[symbol].iloc[index]['Volume'] - kilne_tracker[symbol].iloc[index-1]['Volume']) / kilne_tracker[symbol].iloc[index]['Volume'] * 100,
                'DIF': diff_,
                'DIF_COIN': diff_2,
            }
            global excel_df
            excel_df = excel_df.append(msg, ignore_index=True)

            excel_df.to_csv(f'results/data@zscore-5m-test4.csv', header=True)
    except:
        pass


def init():
    ordersList['list'] = []
    for pair in exchange_pairs:
        ordersList[pair] = {}
        ordersList[pair]['isBuy'] = False


def handle_socket():
    temp = 0
    for key, value in kilne_tracker.items():

        temp = 0

        for index, row in value.iterrows():

            buy(index, symbol=key, time=row.Date, temp=temp)
            sell(price=row.High, low=row.Low, s=key, time=row.Date, index=index)

            temp += 1
            if temp == 12:
                temp = 0

    print(excel_df)


init()

t1 = time.perf_counter()

with concurrent.futures.ThreadPoolExecutor() as executor:

    executor.map(readHistory, exchange_pairs)

t2 = time.perf_counter()


handle_socket()
