from concurrent.futures import ProcessPoolExecutor
import pandas as pd
import os
from backtesting import Backtest, Strategy
from backtesting.lib import crossover
import talib

def resample_ohlcv(dataframe, freq):
    ohlc = dataframe['price'].resample(freq).ohlc()
    volume = dataframe['size'].resample(freq).sum()
    ohlc['Volume'] = volume
    return ohlc.dropna()

def process_file(args):
    filepath, columns_to_remove, freq = args
    df = pd.read_csv(filepath, index_col='ts_event', parse_dates=['ts_event'])
    df.drop(columns=columns_to_remove, inplace=True, errors='ignore')
    df.set_index(pd.to_datetime(df.index), inplace=True)
    return resample_ohlcv(df, freq)

def main():
    directory_path = 'C:/Users/myrcene/Documents/qwantss/Longest Holdings Corporation/enigma/ohlc'
    time_frames = {'1m': '1T', '1h': '1H', '1d': 'D'}
    columns_to_remove = ['rtype', 'publisher_id', 'instrument_id', 'symbol']
    tasks = []

    # Later in your script, when iterating through time_frames:
    for time_frame, pandas_freq in time_frames.items():
        path = os.path.join(directory_path, time_frame)  # Correctly constructs the path
        for filename in os.listdir(path):
            filepath = os.path.join(path, filename)
    dataframes = {}
    with ProcessPoolExecutor() as executor:
        results = executor.map(process_file, tasks)
        for result, task in zip(results, tasks):
            dataframes[task[0]] = result

    # Example of backtesting the first available DataFrame
    if dataframes:
        first_key = next(iter(dataframes))
        class RsiOscillator(Strategy):
            upper_bound = 70
            lower_bound = 30

            def init(self):
                self.rsi = self.I(talib.RSI, self.data.Close, 14)

            def next(self):
                if crossover(self.rsi, self.upper_bound):
                    self.position.close()
                elif crossover(self.lower_bound, self.rsi):
                    self.buy()

        bt = Backtest(dataframes[first_key], RsiOscillator, cash=10_000, commission=.003)
        stats = bt.run()
        print(stats)

if __name__ == "__main__":
    main()
