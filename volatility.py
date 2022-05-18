from cmath import sqrt
from multiprocessing.connection import Client

import pandas as pd

from config import API_KEY, API_SECRET

ordersList = []


class Order:
    def __init__(self, symbol, type, interval, buyPrice, price, amount, startDate, volume, qVolume, buyBlack, buyRed, buyBlue, buyRatio):
        self.symbol,
        self.type,
        self.interval,
        self.buyPrice,
        self.price,
        self.amount,
        self.startDate,
        self.volume,
        self.qVolume,
        self.buyBlack,
        self.buyRed,
        self.buyBlue,
        self.buyRatio


def zScore(window, close, volume):

    mean = (volume*close).rolling(window=window).sum() / \
        volume.rolling(window=window).sum()

    vwapsd = sqrt(pow(close-mean, 2).rolling(window=window).mean())

    return (close-mean)/(vwapsd)


def getData(symbol):

    client = Client(api_key=API_KEY, api_secret=API_SECRET)
    klines = client.get_historical_klines(
        symbol=symbol, interval=Client.KLINE_INTERVAL_5MINUTE, start_str="20 sep 2021")

    data = pd.DataFrame(klines)
    data[0] = pd.to_datetime(data[0], unit='ms')

    data.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'IGNORE', 'Quote_Volume',
                    'Trades_Count', 'BUY_VOL', 'BUY_VOL_VAL', 'x']

    data = data.drop(columns=['IGNORE',
                              'Trades_Count', 'BUY_VOL', 'BUY_VOL_VAL', 'x'])

    data['Close'] = pd.to_numeric(
        data['Close'], errors='coerce')

    close = pd.to_numeric(data['Close'])
    open = pd.to_numeric(data['Open'])
    high = pd.to_numeric(data['High'])
    low = pd.to_numeric(data['Low'])
    volume = pd.to_numeric(data['Volume'])

    data['48-zscore'] = zScore(window=48, close=close, volume=volume)
    data['199-zscore'] = zScore(window=199, close=close, volume=volume)
    data['484-zscore'] = zScore(window=484, close=close, volume=volume)

    a = data['High'].shift(1).rolling(14).max()
    b = data['High'].shift(1).rolling(15).max()
    c = data['Low'].shift(1).rolling(14).min()
    d = data['Low'].shift(1).rolling(15).min()

    l = data['Close'].shift(15)
    max = pd.DataFrame(columns=['max'], data=np.where(a < b, l, a))
    min = pd.DataFrame(columns=['min'], data=np.where(c > d, l, c))

    data['Max'] = max
    data['Min'] = min
    data['volatility ratio'] = (high - low) / (data['Max'] - data['Min'])

    print(data['volatility ratio'])

    df = pd.DataFrame(columns=['symbol',
                               'type',
                               'interval',
                               'buyPrice',
                               'sellPrice',
                               'buyAmount',
                               'gainProfit',
                               'gainAmount',
                               'totalAmount',
                               'startDate',
                               'endDate',
                               'avgDate',
                               'sellVolume',
                               'volume',
                               'quoteVolume',
                               'sellList',
                               'buyBlack',
                               'buyRed',
                               'buyBlue',
                               'buyRatio',
                               'sellBlack',
                               'sellRed',
                               'sellBlue',
                               'sellRetio',
                               ])

    for index, row in data.iterrows():

        if row['48-zscore'] <= -3.5:
            ordersList.append(
                Order(symbol=symbol, type='48', interval='30M', buyPrice=row['Close'], price=[row['Close']], amount=500,
                      startDate=row['Date'], volume=row['Volume'], qVolume=row['Quote_Volume'], buyBlack=pd.to_numeric(
                          row['48-zscore']),
                      buyRed=pd.to_numeric(row['484-zscore']),  buyBlue=pd.to_numeric(row['199-zscore']), buyRatio=pd.to_numeric(row['volatility ratio'])))

        else:
            for order in ordersList:

                rate = ((row['Close'] - order.price[-1]) /
                        order.price[-1]) * 100

                if order.type == '48':

                    if row['48-zscore'] >= 2.0 and rate > 0.0:
                        order.gainProfit += rate
                        order.endDate = row['Date']
                        order.price.append(row['Close'])
                        order.sellList.append(row['Date'])
                        order.sellBlack = pd.to_numeric(row['48-zscore'])
                        order.sellRed = pd.to_numeric(row['484-zscore'])
                        order.sellBlue = pd.to_numeric(row['199-zscore'])
                        order.sellRatio = pd.to_numeric(
                            row['volatility ratio'])

                        new_row = {'symbol': order.symbol,
                                   'type': order.type,
                                   'interval': order.interval,
                                   'buyPrice': order.buyPrice,
                                   'sellPrice': order.price,
                                   'buyAmount': order.amount,
                                   'gainProfit': order.gainProfit,
                                   'gainAmount': order.gainProfit / 100 * order.amount,
                                   'totalAmount': (order.gainProfit / 100 + 1) * order.amount,
                                   'startDate': order.startDate,
                                   'endDate': order.endDate,
                                   'sellVolume': order.sellVolume,
                                   'volume': order.volume,
                                   'quoteVolume': order.qVolume,
                                   'sellList': order.sellList,
                                   'buyBlack': order.buyBlack,
                                   'buyRed': order.buyRed,
                                   'buyBlue': order.buyBlue,
                                   'buyRatio': order.buyRatio,
                                   'sellBlack': order.sellBlack,
                                   'sellRed': order.sellRed,
                                   'sellBlue': order.sellBlue,
                                   'sellRatio': order.sellRatio
                                   }
                        ordersList.remove(order)
                        df = df.append(
                            new_row, ignore_index=True)

    df['holding orders'] = len(ordersList)
    df.to_csv(f'files/data2.csv', index=False,
              header=True, mode='a')
    ordersList.clear()
    # data.to_csv(f'files/data.csv', index=False, header=True)
