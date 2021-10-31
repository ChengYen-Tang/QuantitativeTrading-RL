import sys
import pandas as pd
sys.path.append('./')
from src.data.data_loaders.three_markets_data_loader import *

class Test_csv_lorder:
    def setup_method(self):
        data_loader = three_markets_data_loader()
        self.df = data_loader.load_csv_data('./tests/test_files/BTCUSDT-Spot.csv', './tests/test_files/ETHUSDT-Spot.csv', './tests/test_files/ETHBTC-Spot.csv')

    def test_sort(self):
        results = [self.df['date'][index - 1] < self.df['date'][index] for index in range(1, len(self.df))]
        assert all(results)

    def test_length(self):
        assert len(self.df) == 309557

    def test_data_no_nan(self):
        results = pd.notna(self.df)
        assert all(results)

class Test_private_csv_lorder:
    def setup_method(self):
        data_loader = three_markets_data_loader()
        self.coin1_df, self.coin2_df, self.coin1_2_coin2_df = data_loader._three_markets_data_loader__load_csv_data('./tests/test_files/BTCUSDT-Spot.csv', './tests/test_files/ETHUSDT-Spot.csv', './tests/test_files/ETHBTC-Spot.csv')

    def test_deduplication(self):
        assert len(self.coin1_df) == 309559
        assert len(self.coin2_df) == 309559
        assert len(self.coin1_2_coin2_df) == 309559

    def test_sort(self):
        coin1_results = [self.coin1_df['date'][index - 1] < self.coin1_df['date'][index] for index in range(1, len(self.coin1_df))]
        coin2_results = [self.coin2_df['date'][index - 1] < self.coin2_df['date'][index] for index in range(1, len(self.coin2_df))]
        coin1_2_coin2_results = [self.coin1_2_coin2_df['date'][index - 1] < self.coin1_2_coin2_df['date'][index] for index in range(1, len(self.coin1_2_coin2_df))]
        assert all(coin1_results)
        assert all(coin2_results)
        assert all(coin1_2_coin2_results)
