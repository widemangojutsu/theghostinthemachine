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
