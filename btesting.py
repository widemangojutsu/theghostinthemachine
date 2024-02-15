import pandas as pd
import seaborn as sbn
import matplotlib.pyplot as plt
import numpy as np
import os
import sys
import time
import datetime
import threading
import os
import concurrent.futures
from numba import njit
import talib
from backtesting import Backtest, Strategy
from backtesting.lib import crossover
from backtesting.test import GOOG
import plotly
import datetime as dt


class ICT:
    def __init__(self, data):
        self.data = data
        self.data.reset_index(drop=True, inplace=True)

    def pump(self):
        movement = ((self.data['close'] - self.data['open']) / self.data['open']) * 100
        self.data['pump_move'] = movement
        return movement > 0

    def dump(self):
        movement = ((self.data['open'] - self.data['close']) / self.data['open']) * 100
        self.data['dump_move'] = movement
        return movement > 0

    def shigh(self):
        condition = self.shigh_condition()
        self.data['shigh_price'] = self.data['high'][condition]
        return condition

    def slow(self):
        condition = self.slow_condition()
        self.data['slow_price'] = self.data['low'][condition]
        return condition

    def bisob(self):
        condition = self.slow_condition()
        self.data['bisob_open'] = self.data['open'].shift(1)[condition]
        return condition

    def sibob(self):
        condition = self.shigh_condition()
        self.data['sibob_open'] = self.data['open'].shift(1)[condition]
        return condition

    def bisi(self):
        bisi_indices = ICT.calculate_bisi_njit(self.data['low'].to_numpy(), self.data['high'].to_numpy())
        bisi_series = pd.Series(False, index=self.data.index)
        bisi_series.iloc[bisi_indices] = True
        self.data['bisi_high'] = self.data['high'][bisi_series]
        self.data['bisi_low'] = self.data['low'][bisi_series]
        return bisi_series

    def sibi(self):
        sibi_indices = ICT.calculate_sibi_njit(self.data['low'].to_numpy(), self.data['high'].to_numpy())
        sibi_series = pd.Series(False, index=self.data.index)
        sibi_series.iloc[sibi_indices] = True
        self.data['sibi_low'] = self.data['low'][sibi_series]
        self.data['sibi_high'] = self.data['high'][sibi_series]
        return sibi_series

    def ce(self, value1, value2):
        """
        Calculate the mean of two values.
        
        Parameters:
        value1 (float): The first value.
        value2 (float): The second value.
        
        Returns:
        float: The mean of value1 and value2.
        """
        return (value1 + value2) / 2
        

    @staticmethod
    @njit
    def calculate_bisi_njit(low, high):
        bisi_indices = []
        for i in range(1, len(low) - 1):
            if high[i - 1] < low[i + 1]:
                bisi_indices.append(i)
        return np.array(bisi_indices)

    def calculate_bisi(self):
        low = self.data['low'].to_numpy()
        high = self.data['high'].to_numpy()
        bisi_indices = ICT.calculate_bisi_njit(low, high)

        # Create a Boolean series with the same length as the DataFrame
        bisi_series = pd.Series(False, index=self.data.index)
        bisi_series.iloc[bisi_indices] = True
        return bisi_series

    @staticmethod
    @njit
    def calculate_sibi_njit(low, high):
        sibi_indices = []
        for i in range(1, len(low) - 1):
            if low[i - 1] > high[i + 1]:
                sibi_indices.append(i)
        return np.array(sibi_indices)

    def calculate_sibi(self):
        low = self.data['low'].to_numpy()
        high = self.data['high'].to_numpy()
        sibi_indices = ICT.calculate_sibi_njit(low, high)

        # Create a Boolean series with the same length as the DataFrame
        sibi_series = pd.Series(False, index=self.data.index)
        sibi_series.iloc[sibi_indices] = True
        return sibi_series

    # Helper methods for conditions
    def shigh_condition(self):
        high_shifted_minus_1 = self.data['high'].shift(-1)
        high_shifted_plus_1 = self.data['high'].shift(1)
        return (self.data['high'] > high_shifted_minus_1) & (self.data['high'] > high_shifted_plus_1)

    def slow_condition(self):
        low_shifted_minus_1 = self.data['low'].shift(-1)
        low_shifted_plus_1 = self.data['low'].shift(1)
        return (self.data['low'] < low_shifted_minus_1) & (self.data['low'] < low_shifted_plus_1)


es15 = pd.read_csv('estick15min.csv')
es5 = pd.read_csv('estick5min.csv')
es30 = pd.read_csv('estick_30m.csv')
es60 = pd.read_csv('estick_60m.csv')
es240 = pd.read_csv('estick_240m.csv')
es1 = pd.read_csv('estick1m.csv')

# Convert 'ts_event' to datetime with UTC timezone
es60['ts_event'] = pd.to_datetime(es60['ts_event'], utc=True)

# Now convert the timezone-aware datetime to timezone-naive (removing timezone information)
es60['ts_event'] = es60['ts_event'].dt.tz_convert(None)

# Continue with renaming columns, dropping NaNs, setting the index, and changing column types
es60 = (es60.rename(columns={
            'open': 'Open',
            'high': 'High',
            'low': 'Low',
            'close': 'Close'
        })
        .dropna(subset=['Open', 'High', 'Low', 'Close'])
        .set_index('ts_event')
        .astype({
            'Open': 'float64',
            'High': 'float64',
            'Low': 'float64',
            'Close': 'float64'
        }))


# Check if the index is already a DateTimeIndex
if not isinstance(es60.index, pd.DatetimeIndex):
    # Convert the index to a DateTimeIndex if it's not already
    es60.index = pd.to_datetime(es60.index)

es15.index.name = 'datetime'


def optim_func(series):

    if series["# Trades"] < 100:
        return -1

    return series["Equity Final [$]"] / series["Exposure Time [%]"]


class RsiOscillator(Strategy):
    upper_bound = 70
    lower_bound = 30
    rsi_window = 14
    def init(self):
        self.rsi = self.I(talib.RSI, self.data.Close, self.rsi_window)

    def next(self):
        if crossover(self.rsi, self.upper_bound):
            self.position.close()

        elif crossover(self.lower_bound, self.rsi):
            self.buy()


bt = Backtest(es60, RsiOscillator, commission=.002, cash=10000)
# stats = bt.run()
# print(stats)
# bt.plot()

stats = bt.optimize(
    upper_bound=range(60, 80, 5),
    lower_bound=range(20, 40, 5),
    rsi_window=range(10, 20, 2),
    constraint=lambda p: p.upper_bound > p.lower_bound,
    maximize = optim_func
)
print(stats)

bt.plot()