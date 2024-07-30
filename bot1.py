from datetime import datetime

from lumibot.backtesting import BacktestingBroker, PolygonDataBacktesting
from lumibot.strategies import Strategy
from lumibot.traders import Trader


class BuyAndHold(Strategy):
    parameters = {
        "symbol": "AAPL",
    }

    def initialize(self):
        self.sleeptime = "1D"

    def on_trading_iteration(self):
        if self.first_iteration:
            symbol = self.parameters["symbol"]
            price = self.get_last_price(symbol)
            qty = self.portfolio_value / price
            order = self.create_order(symbol, quantity=qty, side="buy")
            self.submit_order(order)


if __name__ == "__main__":

    is_live = False

    if is_live:
        from credentials import ALPACA_CONFIG

        from lumibot.brokers import Alpaca
        from lumibot.traders import Trader

        trader = Trader()

        broker = Alpaca(ALPACA_CONFIG)

        strategy = BuyAndHold(broker=broker)

        trader.add_strategy(strategy)
        strategy_executors = trader.run_all()

    else:

        backtesting_start = datetime(2023, 1, 1)
        backtesting_end = datetime(2023, 5, 1)

        trader = Trader(backtest=True)
        data_source = PolygonDataBacktesting(
            datetime_start=backtesting_start,
            datetime_end=backtesting_end,
            api_key="NrThd7hnOg9g9mKjTiqHuKdmJ9rQ7ZZQ",
        )
        broker = BacktestingBroker(data_source)
        my_strat = BuyAndHold(
            broker=broker,
            benchmark_asset="SPY",
        )
        trader.add_strategy(my_strat)
        trader.run_all()












