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
        max_value = 0
        min_value = 0
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
                max_data = df[['Close_ROC', 'Volume_ROC', 'Close_ROC_15', 'Volume_ROC_15', 'Close_ROC_60', 'Volume_ROC_60', 'Close_ROC_240', 'Volume_ROC_240', 'Close_ROC_1440', 'Volume_ROC_1440']].values.max()
                min_data = df[['Close_ROC', 'Volume_ROC', 'Close_ROC_15', 'Volume_ROC_15', 'Close_ROC_60', 'Volume_ROC_60', 'Close_ROC_240', 'Volume_ROC_240', 'Close_ROC_1440', 'Volume_ROC_1440']].values.min()
                if max_data > max_value:
                    max_value = max_data
                if min_data < min_value:
                    min_value = min_data
        return all_data, max_value, min_value

    def data_processing(self, file_path):
        stock_code, df = self.data_loader.LoadCsv(file_path)
        if stock_code == None:
            return None, None
            
        df['Close_ROC'] = talib.ROC(df['close'], timeperiod=1)
        df['Volume_ROC'] = talib.ROC(df['volume'], timeperiod=1)
        df['Close_ROC_15'] = talib.ROC(df['close'], timeperiod=15)
        df['Volume_ROC_15'] = talib.ROC(df['volume'], timeperiod=15)
        df['Close_ROC_60'] = talib.ROC(df['close'], timeperiod=60)
        df['Volume_ROC_60'] = talib.ROC(df['volume'], timeperiod=60)
        df['Close_ROC_240'] = talib.ROC(df['close'], timeperiod=240)
        df['Volume_ROC_240'] = talib.ROC(df['volume'], timeperiod=240)
        df['Close_ROC_1440'] = talib.ROC(df['close'], timeperiod=1440)
        df['Volume_ROC_1440'] = talib.ROC(df['volume'], timeperiod=1440)
        df = df.drop(['stock_code', 'open', 'high', 'low', 'volume', 'money', 'factor', 'change', 'TradeCount' , 'TakerBuyBaseVolume', 'TakerBuyQuoteVolume'], axis=1)
        drop_index = [index for index in range(1440)]
        df.drop(drop_index, inplace=True)
        return stock_code, df.reset_index()

if __name__ == '__main__':
    data_loader = AllROCDataLoader('2017-10-17 04:01:00', '2017-10-19 05:10:00')
    result, max_value, min_value = data_loader.load_data('./tests/test_files')
    print(result.keys())
    print(result['BTCUSDT'])
    print(result['ETHBTC'])
    print(result['ETHUSDT'])
    print(max_value)
    print(min_value)
