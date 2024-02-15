
from backtesting import Backtest, Strategy, crossover
from backtesting.test import GOOG
import tablib


print(GOOG)

class RsiOscillator(Strategy):
    
    upper_bound = 70
    lower_bound = 30
    
    
    
    def init(self):
        self.rsi = self.I(RSI, self.data.Close, 14)

    def next(self):
        if crossover(self.rsi, self.upper_bound):
            self.position.close()

        elif crossover(self.lower_bound, self.rsi):
            self.buy()



bt = Backtest(GOOG, RsiOOscillator, cash = 10_000, commission = .003)

stats = bt.run()