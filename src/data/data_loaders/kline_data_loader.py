import pandas as pd

class kline_data_loader:
    def LoadCsv(self, path):
        return self.__LoadCsv(path)

    def __LoadCsv(self, path):
        df = pd.read_csv(path)
        df['date'] = df['date'].astype('datetime64')
        df = df.drop_duplicates(subset=['date'])
        df = df.sort_values(by=['date'])
        return df.reset_index()

if __name__ == '__main__':
    data_loader = kline_data_loader()
    df = data_loader.LoadCsv('/Users/kenneth/OneDrive - 臺北科技大學 軟體工程實驗室/量化交易/General/原始資料集/BTCUSDT-Spot.csv')
    print(df)
    print(df.dtypes)
