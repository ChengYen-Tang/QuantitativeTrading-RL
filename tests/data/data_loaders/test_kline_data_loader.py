import pytest
from src.data.data_loaders.kline_data_loader import *

def setup_function():
    data_loader = kline_data_loader()
    pytest.df = data_loader.LoadCsv('./tests/test_files/BTCUSDT-Spot.csv')

def test_deduplication():
    assert len(pytest.df) == 309559

def test_sort():
    results = [pytest.df['date'][index - 1] < pytest.df['date'][index] for index in range(1, len(pytest.df))]
    assert all(results)
