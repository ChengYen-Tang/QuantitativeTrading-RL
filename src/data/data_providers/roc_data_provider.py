import sys
sys.path.append('./')
import numpy as np
from src.data.data_loaders.all_roc_data_loader import *

class RocDataProvider:
    def __init__(self, folder_path, start_time, end_time, time_cycle, base_symbol):
        self.data_loader = AllROCDataLoader(start_time, end_time)
        self.data, self.__max_value, self.__min_value = self.data_loader.load_data(folder_path)
        self.time_cycle = time_cycle
        cache_folder_path = './cache/'
        observation_cache_path = os.path.join(cache_folder_path, 'observation_cache.npy')
        trading_symbols_cache_path = os.path.join(cache_folder_path, 'trading_symbols_cache.npy')
        if os.path.isfile(observation_cache_path):
            self.observation_sort = np.load(observation_cache_path)
        else:
            self.observation_sort = np.array(list(self.data.keys()))
            if not os.path.exists(cache_folder_path):
                os.mkdir(cache_folder_path)
            np.save(observation_cache_path, self.observation_sort)
        if os.path.isfile(trading_symbols_cache_path):
            self.trading_symbols = np.load(trading_symbols_cache_path)
        else:
            self.trading_symbols = self.observation_sort[np.flatnonzero(np.core.defchararray.find(self.observation_sort, base_symbol)!=-1)]
            np.save(trading_symbols_cache_path, self.trading_symbols)
        if (len(self.data[self.observation_sort[0]]) - 1) % time_cycle is not 0:
            raise ValueError('時間範圍內的資料數量減去前 1440 及最後一筆資料後，必須可以被 time_cycle 整除')

    def reset_cycle(self):
        self.cycle_start_index = 0
        self.__set_cycle_end_index()

    def move_next_cycle(self):
        if self.cycle_end_index >= len(self.data[self.observation_sort[0]]) - 2:
            return False
        self.cycle_start_index = self.cycle_end_index + 1
        self.__set_cycle_end_index()
        self.index = self.cycle_start_index
        return True

    def __set_cycle_end_index(self):
        self.cycle_end_index = self.cycle_start_index + self.time_cycle - 1

    def reset(self):
        self.index = self.cycle_start_index

    def move_next(self):
        if self.index >= self.cycle_end_index:
            return False
        self.index += 1
        return True

    def get_observation(self):
        observation = []
        for stock_code in self.observation_sort:
            observation.append(self.data[stock_code][['Close_ROC', 'Volume_ROC', 'Close_ROC_15', 'Volume_ROC_15', 'Close_ROC_60', 'Volume_ROC_60', 'Close_ROC_240', 'Volume_ROC_240', 'Close_ROC_1440', 'Volume_ROC_1440']].values[self.index])
        return np.array(observation)

    def get_close_by_stock_code(self, stock_code):
        return self.data[stock_code]['close'].values[self.index]

    def get_next_close_by_stock_code(self, stock_code):
        return self.data[stock_code]['close'].values[self.index]

    def get_observation_shape(self):
        return self.get_observation().shape

    def is_last_data_of_the_day(self):
        return (self.index + 1) % 1440 is 0

    def get_current_time(self):
        return self.data[self.observation_sort[0]]['date'].values[self.index]

    def get_trading_stock_code(self):
        return self.trading_symbols

    def get_observation_max_min(self):
        return self.__max_value, self.__min_value

if __name__ == '__main__':
    data_loader = RocDataProvider('./tests/test_files', '2017-10-14 00:00:00', '2017-11-19 00:00:00', 10080, 'USDT')
    data_loader.reset_cycle()
    data_loader.reset()
    for a in range(4):
        data_loader.move_next_cycle()
    # for a in range(1440*10):
    #     data_loader.move_next()
    print(data_loader.get_current_time())
    print(data_loader.is_last_data_of_the_day())
    print(data_loader.get_close_by_stock_code('BTCUSDT'))
    print(data_loader.get_next_close_by_stock_code('BTCUSDT'))