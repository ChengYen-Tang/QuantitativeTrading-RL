import sys
sys.path.append('./')
import talib
import os
from multiprocessing import Pool
from src.data.data_loaders.kline_data_loader import *

class AllROCDataLoader():
    def __init__(self, start_time, end_time):
        self.data_loader = KlineDataLoader(start_time, end_time)

    def load_data(self, folder_path):
        process_pool = Pool()
        results = []
        for file in os.listdir(folder_path):
            results.append(process_pool.apply_async(self.data_processing, [os.path.join(folder_path, file)]))
        process_pool.close()
        process_pool.join()

        all_data = {}
        for item in results:
            stock_code, df = item.get()
            if stock_code is not None:
                all_data[stock_code] = df
        return all_data

    def data_processing(self, file_path):
        self.stock_code, self.df = self.data_loader.LoadCsv(file_path)
        if self.stock_code == None:
            return None, None
            
        self.df['Close_ROC'] = talib.ROC(self.df['close'], timeperiod=1)
        self.df['Volume_ROC'] = talib.ROC(self.df['volume'], timeperiod=1)
        self.df['Close_ROC_15'] = talib.ROC(self.df['close'], timeperiod=15)
        self.df['Volume_ROC_15'] = talib.ROC(self.df['volume'], timeperiod=15)
        self.df['Close_ROC_60'] = talib.ROC(self.df['close'], timeperiod=60)
        self.df['Volume_ROC_60'] = talib.ROC(self.df['volume'], timeperiod=60)
        self.df = self.df.drop(['stock_code', 'open', 'high', 'low', 'close', 'volume', 'money', 'factor', 'change', 'TradeCount' , 'TakerBuyBaseVolume', 'TakerBuyQuoteVolume'], axis=1)
        return self.stock_code, self.df

if __name__ == '__main__':
    data_loader = AllROCDataLoader('2017-10-17 04:01:00', '2017-10-17 05:10:00')
    result = data_loader.load_data('./tests/test_files')
    print(result.keys())
    print(result['BTCUSDT'])
    print(result['ETHBTC'])
    print(result['ETHUSDT'])
