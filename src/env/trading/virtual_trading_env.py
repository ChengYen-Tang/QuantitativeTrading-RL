class VirtualTradingEnv:
    def __init__(self, default_balance, fee, stock_code, data_provider):
        self.default_balance = default_balance
        self.fee = fee / 100
        self.stock_code = stock_code
        self.data_provider = data_provider

    def reset(self):
        self.balance = self.default_balance
        self.coin_count = 0
        self.is_hold = False

    def trading(self, is_hold):
        if is_hold and not self.is_hold:
            self.__buy()
        elif not is_hold and self.is_hold:
            self.__sell()
            
    def __buy(self):
        buy_count = self.balance / self.data_provider.get_close_by_stock_code(self.stock_code)
        handlingCost = buy_count * self.fee
        self.coin_count = buy_count - handlingCost
        self.balance = 0
        self.is_hold = True

    def __sell(self):
        sell_income = self.coin_count * self.data_provider.get_close_by_stock_code(self.stock_code)
        handling_cost = sell_income * self.fee
        self.balance = sell_income - handling_cost
        self.coin_count = 0
        self.is_hold = False

    def get_income_roc(self):
        if self.is_hold:
            income = self.coin_count * self.data_provider.get_next_close_by_stock_code(self.stock_code)
        else:
            income = self.balance

        return ((income / self.default_balance) - 1) * 100
