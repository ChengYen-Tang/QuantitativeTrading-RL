from src.data.data_loaders.kline_data_loader import *
from dateutil.relativedelta import relativedelta

class TestOutOfDate:
    def setup(self):
        data_loader = KlineDataLoader('2016-08-17 04:01:00', '2016-08-17 04:10:00')
        self.stock_code, self.df = data_loader.LoadCsv('./tests/test_files/BTCUSDT-Spot.csv')

    def test_load(self):
        assert self.stock_code == None

class TestLoadData:
    def setup(self):
        data_loader = KlineDataLoader('2017-08-17 04:01:00', '2017-08-17 04:10:00')
        self.stock_code, self.df = data_loader.LoadCsv('./tests/test_files/BTCUSDT-Spot.csv')

    def test_data_arrangementn(self):
        assert len(self.df) == 10
        assert self.stock_code == 'BTCUSDT'

    def test_sort(self):
        results = [self.df['date'][index - 1] + relativedelta(minutes=1) == self.df['date'][index] for index in range(1, len(self.df))]
        assert all(results)
