import pandas as pd

class kline_data_loader:
    def __init__(self, start_time, end_time):
        self.start_time = pd.to_datetime(start_time)
        self.end_time = pd.to_datetime(end_time)

    def LoadCsv(self, path):
        return self.__LoadCsv(path)

    def __LoadCsv(self, path):
        df = pd.read_csv(path)
        df['date'] = df['date'].astype('datetime64')
        if df['date'].max() < self.end_time or df['date'].min() > self.start_time:
            return None
        # 刪除指定時間範圍外的資料
        drop_index = df[df['date'] < self.start_time].index
        df.drop(drop_index, inplace=True)
        drop_index = df[df['date'] > self.end_time].index
        df.drop(drop_index, inplace=True)

        df = df.drop_duplicates(subset=['date'])
        df = df.sort_values(by=['date'])
        # 補齊缺少的資料
        t_index = pd.date_range(self.start_time, self.end_time, freq='min')
        date_df = pd.DataFrame(t_index, columns= ['date'])
        df = df.merge(date_df, how='right', sort=True)
        df = df.fillna(method='ffill', axis=0)
        
        return df['stock_code'].iloc[0].split('-')[0], df.reset_index()

if __name__ == '__main__':
    data_loader = kline_data_loader('2017-08-17 04:01:00', '2017-08-17 04:10:00')
    stock_code, df = data_loader.LoadCsv('./tests/test_files/BTCUSDT-Spot.csv')
    print(stock_code)
    print(df)
