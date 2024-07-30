from datetime import datetime

from lumibot.entities import Asset
from lumibot.strategies.strategy import Strategy

"""
Strategy Description

An example strategy for buying an option and holding it to expiry.
"""


class OptionsHoldToExpiry(Strategy):
    parameters = {
        "buy_symbol": "SPY",
        "expiry": datetime(2023, 10, 20),
    }

    # =====Overloading lifecycle methods=============

    def initialize(self):
        # Set the initial variables or constants

        # Built in Variables
        self.sleeptime = "1M"
        self.isOpen = True

        self.open

    def on_trading_iteration(self):
        """Buys the self.buy_symbol once, then never again"""

        buy_symbol = self.parameters["buy_symbol"]
        expiry = self.parameters["expiry"]

        # What to do each iteration
        underlying_price = self.get_last_price(buy_symbol)
        self.log_message(f"The value of {buy_symbol} is {underlying_price}")


        # Await market to open (on_trading_iteration will stop running until the market opens)
        self.await_market_to_open()
        
        if self.first_iteration:
            # Calculate the strike price (round to nearest 1)
            strike = round(underlying_price)

            # Create options asset
            asset = Asset(
                symbol=buy_symbol,
                asset_type="option",
                expiration=expiry,
                strike=strike,
                right="call",
            )

            # Create order
            order = self.create_order(
                asset,
                10,
                "buy_to_open",
            )
            
            # Submit order
            self.submit_order(order)

            # Log a message
            self.log_message(f"Bought {order.quantity} of {asset}")

        else:
            pass


    def call_put_strike(self, last_price, symbol, expiration_date):
        """Returns strikes for pair."""

        buy_call_strike = 0
        buy_put_strike = 0

        # What to do each iteration
        underlying_price = self.get_last_price(symbol)
        self.log_message(f"The value of {symbol} is {underlying_price}")
        
        strike = round(underlying_price)
    
        asset = self.create_asset(
            symbol=symbol, 
            asset_type="option",
            expiration=expiration_date,
            strike=strike,
            right="call",         
        )

        strikes = self.get_strikes(asset)

        for strike in strikes:
            if strike < last_price:
                buy_put_strike = strike
                buy_call_strike = strike
            elif strike > last_price and buy_call_strike < last_price:
                buy_call_strike = strike
            elif strike > last_price and buy_call_strike > last_price:
                break

        return buy_call_strike, buy_put_strike

    def get_expiration_date(self, expirations):
        """Expiration date that is closest to, but less than max days to expriry. """
        expiration_date = None
        # Expiration
        current_date = datetime.now().date()
        for expiration in expirations:
            ex_date = expiration

            exp_date = datetime.strptime(ex_date, '%Y-%m-%d').date()

            
            net_days = (exp_date - current_date).days
            if net_days < self.max_days_expiry:
                expiration_date = expiration

        return expiration_date
    


if __name__ == "__main__":
    is_live = False

    if is_live:
        from credentials import INTERACTIVE_BROKERS_CONFIG

        from lumibot.brokers import InteractiveBrokers
        from lumibot.traders import Trader

        trader = Trader()

        broker = InteractiveBrokers(INTERACTIVE_BROKERS_CONFIG)
        strategy = OptionsHoldToExpiry(broker=broker)

        trader.add_strategy(strategy)
        strategy_executors = trader.run_all()

    else:
        from lumibot.backtesting import PolygonDataBacktesting

        # Backtest this strategy
        backtesting_start = datetime(2023, 10, 19)
        backtesting_end = datetime(2023, 10, 24)

        results = OptionsHoldToExpiry.backtest(
            PolygonDataBacktesting,
            backtesting_start,
            backtesting_end,
            benchmark_asset="SPY",
            polygon_api_key="NrThd7hnOg9g9mKjTiqHuKdmJ9rQ7ZZQ",  # Add your polygon API key here
        )