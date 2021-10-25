import pytest
from src.data.data_loaders.three_markets_data_loader import *

def setup_function():
    data_loader = three_markets_data_loader()
    pytest.coin1_df, pytest.coin2_df, pytest.coin1_2_coin2_df = data_loader.load_csv_data('./tests/test_files/BTCUSDT-Spot.csv', './tests/test_files/ETHUSDT-Spot.csv', './tests/test_files/ETHBTC-Spot.csv')

def test_deduplication():
    print(pytest.coin1_df)
    assert len(pytest.coin1_df) == 309559
    assert len(pytest.coin2_df) == 309559
    assert len(pytest.coin1_2_coin2_df) == 309559

def test_sort():
    coin1_results = [pytest.coin1_df['date'][index - 1] < pytest.coin1_df['date'][index] for index in range(1, len(pytest.coin1_df))]
    coin2_results = [pytest.coin2_df['date'][index - 1] < pytest.coin2_df['date'][index] for index in range(1, len(pytest.coin2_df))]
    coin1_2_coin2_results = [pytest.coin1_2_coin2_df['date'][index - 1] < pytest.coin1_2_coin2_df['date'][index] for index in range(1, len(pytest.coin1_2_coin2_df))]
    assert all(coin1_results)
    assert all(coin2_results)
    assert all(coin1_2_coin2_results)
