import sys
sys.path.append('./')
from multiprocessing import Pool
from src.data.data_loaders.kline_data_loader import *

class three_markets_data_loader(kline_data_loader):
    def load_csv_data(self, coin1_path, coin2_path, coin1_2_coin2_path):
        coin1_df, coin2_df, coin1_2_coin2_df = self.__load_csv_data(coin1_path, coin2_path, coin1_2_coin2_path)
        df = coin1_df.merge(coin2_df, on='date', suffixes=('_coin1', '_coin2'))
        df = df.merge(coin1_2_coin2_df, on='date', suffixes=('', '_coin1_2_coin2'), sort=True)
        return df

    def __load_csv_data(self, coin1_path, coin2_path, coin1_2_coin2_path):
        process_pool = Pool()
        result = [process_pool.apply_async(self.LoadCsv, [coin1_path]),
            process_pool.apply_async(self.LoadCsv, [coin2_path]),
            process_pool.apply_async(self.LoadCsv, [coin1_2_coin2_path])]
        process_pool.close()
        process_pool.join()
        return result[0].get(), result[1].get(), result[2].get()

if __name__ == '__main__':
    data_loader = three_markets_data_loader()
    coin1_df, coin2_df, coin1_2_coin2_df = data_loader._three_markets_data_loader__load_csv_data('./tests/test_files/BTCUSDT-Spot.csv', './tests/test_files/ETHUSDT-Spot.csv', './tests/test_files/ETHBTC-Spot.csv')
    print(coin1_df)
    print(coin2_df)
    print(coin1_2_coin2_df)
