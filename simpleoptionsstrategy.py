import logging
import time
from itertools import cycle

import pandas as pd

from lumibot.entities import Asset, TradingFee
from lumibot.strategies.strategy import Strategy
from lumibot.traders import Trader

# importing the alpaca broker class
from lumibot.brokers import Alpaca
import config as ALPACA_CONFIG

from datetime import datetime, timedelta


class SimpleOptionStrategy(Strategy):
    """Strategy Description: Strangle

    In a long strangle—the more common strategy—the investor simultaneously buys an
    out-of-the-money call and an out-of-the-money put option. The call option's strike
    price is higher than the underlying asset's current market price, while the put has a
    strike price that is lower than the asset's market price. This strategy has large profit
    potential since the call option has theoretically unlimited upside if the underlying
    asset rises in price, while the put option can profit if the underlying asset falls.
    The risk on the trade is limited to the premium paid for the two options.

    Place the strangle two weeks before earnings announcement.

    params:
    - take_profit_threshold (float): Percentage to take profit.
    - sleeptime (int): Number of minutes to wait between trading iterations.
    - total_trades (int): Tracks the total number of pairs traded.
    - max_trades (int): Maximum trades at any time.
    - max_days_expiry (int): Maximum number of days to to expiry.
    - days_to_earnings_min(int): Minimum number of days to earnings.
    - exchange (str): Exchange, defaults to `SMART`

    - symbol_universe (list): is the stock symbols expected to have a sharp movement in either direction.
    - trading_pairs (dict): Used to track all information for each symbol/options.
    """

    IS_BACKTESTABLE = True

    # =====Overloading lifecycle methods=============

    def initialize(self):
        self.time_start = time.time()
        # Set how often (in minutes) we should be running on_trading_iteration

        # Initialize our variables
        self.take_profit_threshold = 0.001  # 0.015
        self.sleeptime = "1H"
        self.total_trades = 0
        self.max_trades = 4
        self.max_days_expiry = 15
        self.days_to_earnings_min = 100  # 15
        self.exchange = "SMART"
        self.strike_width = 10

        self.trade_calls = True
        self.trade_puts = True

        self.all_buy_signals = dict()

        # Stock expected to move.
        #self.symbols_universe = ['AAPL', 'MSFT', 'AMD', 'META', 'NVDA', 'SPY', 'QQQ', 'TSLA', 'NFLX']
        #self.symbols_universe = ['AAPL', 'MSFT']
        #self.symbols_universe = ['AAPL', 'MSFT', 'AMD', 'META', 'NVDA']
        self.symbols_universe = ['AAPL', 'MSFT', 'AMD', 'META', 'NVDA', 'TSLA', 'NFLX']

        # Underlying Asset Objects.
        self.trading_pairs = dict()
        for symbol in self.symbols_universe:
            self.create_trading_pair(symbol)        

    def thisFriday(self, referencedate):
        today = None
        if referencedate is not None:
            today = referencedate
        else:
            today = datetime.today()
        
        next_friday = today + timedelta(5-today.weekday())      
        return next_friday

    def get_options_prices(self, asset, options, right, width, beforedate):

        # We're only interested in options chains that expire before this Friday
        multiplier = self.get_multiplier(options["chains"])
                
        strike_expiration = {}
        strike_greeks_calls = {}
        for index, values in options['chains']['Chains'][str(right).upper()].items():
                        
            if datetime.strptime(index, '%Y-%m-%d').date() > beforedate:
                break
            else:

                try:               
                    last_price = self.get_last_price(asset)
                    options["underlying_price"] = last_price
                    assert last_price != 0
                except:
                    logging.warning(f"Unable to get price data for {asset.symbol}.")
                    options["underlying_price"] = 0
                    continue 

                strike_price = 0
                previous_strike_price = 0
                strike_index = 0
                atm_index = 0
                strike_price = 0
                for strike in values:
                    strike_price = round(strike)
                    if previous_strike_price <= round(options["underlying_price"]) and strike_price > round(options["underlying_price"]):
                        atm_index = strike_index
                        break
                    previous_strike_price = strike
                    strike_index += 1

                #10 strikes - 5 above and 5 below
                upper = values[atm_index+width]
                lower = values[atm_index-width]

                for strike_price in values:                                    
                    if strike_price > lower and strike_price < upper:                        
                        asset = Asset(asset.symbol, expiration=datetime.strptime(index, '%Y-%m-%d').date(), strike=str(strike_price), asset_type="option", right=right, multiplier=multiplier)
                                            
                        calls = self.get_last_price(asset)
                        if calls is not None:
                            strike_greeks_calls[strike_price] = calls

                        #calls = self.get_greeks(call_asset, query_greeks=True)   
                        #if calls["option_price"] is not None:
                        #    calls["right"] = "call"
                        #    strike_greeks_calls[strike_price] = calls
                    
                            strike_expiration[index] = strike_greeks_calls

        options[right] = strike_expiration
        return strike_expiration

    def before_starting_trading(self):
        """Create the option assets object for each underlying. """
        self.asset_gen = self.asset_cycle(self.trading_pairs.keys())
        friday_date = self.thisFriday(self.get_datetime()).date()

        print(
            f"**** Before starting trading ****\n"
            f"Time: {self.get_datetime()} " 
            f"Cash: {self.cash}, Value: {self.portfolio_value}  "
            f"Watching contracts which expire before: {friday_date} "
            f"*******  END ELAPSED TIME  "
            f"{(time.time() - self.time_start):5.0f}   "
            f"*******"
        )
        pass

        for asset, options in self.trading_pairs.items():      
            try:
                options["chains"] = self.get_chains(asset)
            except Exception as e:
                logging.info(f"Error: {e}")
                continue

            try:               
                last_price = self.get_last_price(asset)
                options["open_price"] = last_price
                assert last_price != 0
            except:
                logging.warning(f"Unable to get price data for {asset.symbol}.")
                options["open_price"] = 0
                continue      
  
            if self.trade_calls:
                calls = self.get_options_prices(asset, options, "call", self.strike_width, friday_date)
                if calls is not None:
                    options["open_calls"] = calls
                    options["last_calls"] = calls
                    #print(f"{asset.symbol} {calls}")

            if self.trade_puts:        
                puts = self.get_options_prices(asset, options, "put", self.strike_width, friday_date)
                if puts is not None:
                    options["open_puts"] = puts
                    options["last_puts"] = puts
                    #print(f"{asset.symbol} {puts}")

            pass
        
    def get_differences(self, last_calls, calls):
        differences = dict()
        difference = False
        
        for call_index, call_value in calls.items():
            
            strike_differences = dict()
            try:
                last_strikes = last_calls[call_index]
                print (last_strikes)

                strikes = calls[call_index]
                print (strikes)

                for strike_index, strike_value in strikes.items():
                    try:
                        last_strike_value = last_strikes[strike_index]
                        if strike_value != last_strike_value:
                            strike_differences[strike_index] = strike_value            
                    except:
                        pass                                                                
            except:
                pass

            differences[call_index] = strike_differences
        
        print (differences)
        return differences

    def get_buy_signals(self, open_calls, calls):
        differences = dict()
        difference = False
        
        for call_index, call_value in calls.items():
            
            strike_differences = dict()
            try:
                open_strikes = open_calls[call_index]
                #print (open_strikes)

                strikes = calls[call_index]
                #print (strikes)

                for strike_index, strike_value in strikes.items():
                    try:
                        open_strike_value = open_strikes[strike_index]
                        if strike_value > open_strike_value:
                            strike_differences[strike_index] = strike_value - open_strike_value
                    except:
                        pass                                                                
            except:
                pass

            differences[call_index] = strike_differences
        
        #print (differences)
        return differences
    

    def get_buy_signalsNG(self, open_calls, calls, symbol, right):
        differences = dict()
        difference = False

        strike_signals = dict()

        for call_index, call_value in calls.items():
            
            strike_differences = dict()
            try:
                open_strikes = open_calls[call_index]
                strikes = calls[call_index]

                for strike_index, strike_value in strikes.items():
                    try:
                        open_strike_value = open_strikes[strike_index]
                        if strike_value > open_strike_value:
                            strike_signals[symbol + str(strike_index) + right] = strike_value - open_strike_value
                    except:
                        pass                                                                
            except:
                pass

        return strike_signals


    def get_sell_signals(self, open_calls, calls, symbol, right):
        differences = dict()
        difference = False

        strike_signals = dict()

        for call_index, call_value in calls.items():
            
            strike_differences = dict()
            try:
                open_strikes = open_calls[call_index]
                strikes = calls[call_index]

                for strike_index, strike_value in strikes.items():
                    try:
                        open_strike_value = open_strikes[strike_index]
                        if strike_value < open_strike_value:                            
                            strike_signals[symbol + str(strike_index) + right] = strike_value - open_strike_value
                        elif strike_value > (open_strike_value * 2):
                            strike_signals[symbol + str(strike_index) + right] = strike_value - open_strike_value
                        
                            pass
                    except:
                        pass                                                                
            except:
                pass

        return strike_signals



    def on_trading_iteration(self):
        value = self.portfolio_value
        cash = self.cash
        positions = self.get_tracked_positions()
        filled_assets = [p.asset for p in positions]
        trade_cash = self.portfolio_value / (self.max_trades * 2)
        friday_date = self.thisFriday(self.get_datetime()).date()
        
        calls = {}
        buy_signals = None
        buy_call_signals = None
        buy_call_signals = None

        buy_put_signals = None
        buy_call_signals = None

        sell_put_signals = None
        sell_call_signals = None

        # Await market to open (on_trading_iteration will stop running until the market opens)
        #self.await_market_to_open()
  
        for asset, options in self.trading_pairs.items():        
            #print(f"{asset.symbol}")

            # See if we've got new highs
            if self.trade_calls:
                calls = self.get_options_prices(asset, options, "call", self.strike_width, friday_date)
                
                if (options["last_calls"] == None):
                    options["last_calls"] = options["open_calls"]
                #differences = self.get_differences(options["last_calls"], calls)
                buy_call_signals = self.get_buy_signals(options["open_calls"], calls)
                buy_call_signalsNG = self.get_buy_signalsNG(options["open_calls"], calls, asset.symbol, "C")
                sell_call_signals = self.get_sell_signals(options["open_calls"], calls, asset.symbol, "C")
                
                # Save the old set of calls
                options["last_calls"] = calls                
                #print(f"{asset.symbol} {calls}")

            if self.trade_puts:            
                puts = self.get_options_prices(asset, options, "put", self.strike_width, friday_date)
                if (options["last_puts"] == None):
                    options["last_puts"] = options["open_puts"]
                #differences = self.get_differences(options["last_puts"], puts)
                buy_put_signals = self.get_buy_signals(options["open_puts"], puts)
                buy_put_signalsNG = self.get_buy_signalsNG(options["open_puts"], puts, asset.symbol, "P")
                sell_put_signals = self.get_sell_signals(options["open_puts"], puts, asset.symbol, "C")

                # Save the old set of puts
                options["last_puts"] = puts
                #print(f"{asset.symbol} {puts}")


            ###
            # Sell 
            ###
            if self.trade_calls:
                for index, signal in sell_call_signals.items():
                    try:
                        sell_this = self.all_buy_signals[index]
                        pass

                        _asset = Asset(
                            symbol=sell_this.symbol,
                            asset_type=sell_this.asset_type,
                            expiration=sell_this.expiration,
                            strike=sell_this.strike,
                            right=sell_this.right,
                        )

                        # Submit order
                        self.submit_sell_order(_asset)
                        del self.all_buy_signals[index]   

                    except:
                        pass


            if self.trade_puts:
                for index, signal in sell_put_signals.items():
                    try:
                        sell_this = self.all_buy_signals[index]
                        pass

                        _asset = Asset(
                            symbol=sell_this.symbol,
                            asset_type=sell_this.asset_type,
                            expiration=sell_this.expiration,
                            strike=sell_this.strike,
                            right=sell_this.right,
                        )
                        
                        # Submit order
                        self.submit_sell_order(_asset)
                        del self.all_buy_signals[index]                        
                        
                    except:
                        pass

                
            ###
            # Buy 
            ###
            if self.trade_calls:
                for index, signal in buy_call_signals.items():
                    # Calculate the strike price (round to nearest 1)                
                    underlying_price = self.get_last_price(asset)
                    self.log_message(f"The value of {asset} is {underlying_price}")
                    strike = round(underlying_price)

                    try:
                        buy_signal = self.all_buy_signals[asset.symbol + str(strike) + "C"]
                        pass
                    except:
                        expiry = datetime.strptime(next(iter(options["last_calls"])), '%Y-%m-%d').date()
                        # Create options asset
                        _asset = Asset(
                            symbol=asset.symbol,
                            asset_type="option",
                            expiration=expiry,
                            strike=strike,
                            right="call",
                        )

                        self.submit_buy_order(_asset)
                        self.all_buy_signals[asset.symbol + str(strike) + "C"] = _asset

            if self.trade_puts:
                for index, signal in buy_put_signals.items():
                    # Calculate the strike price (round to nearest 1)                
                    underlying_price = self.get_last_price(asset)
                    self.log_message(f"The value of {asset} is {underlying_price}")
                    strike = round(underlying_price)

                    try:
                        buy_signal = self.all_buy_signals[asset.symbol + str(strike) + "P"]
                        pass
                    except:
                        expiry = datetime.strptime(next(iter(options["last_puts"])), '%Y-%m-%d').date()
                        # Create options asset
                        _asset = Asset(
                            symbol=asset.symbol,
                            asset_type="option",
                            expiration=expiry,
                            strike=strike,
                            right="put",
                        )

                        self.submit_buy_order(_asset)
                        self.all_buy_signals[asset.symbol + str(strike) + "P"] = _asset



        positions = self.get_tracked_positions()
        filla = [pos.asset for pos in positions]
        print(
            f"**** End of iteration ****\n"
            f"Time: {self.get_datetime()} "
            f"Cash: {self.cash}, Value: {self.portfolio_value}  "
            f"Positions: {positions} "
            f"Filled_assets: {filla} "
            f"*******  END ELAPSED TIME  "
            f"{(time.time() - self.time_start):5.0f}   "
            f"*******"
        )

        pass

        # self.await_market_to_close()

    def submit_buy_order(self, asset):
        # Create order
        order = self.create_order(
            asset,
            1,
            "buy_to_open",
        )
    
        # Submit order
        self.submit_order(order)

        # Log a message
        message = (f"Bought {order.quantity} of {asset}")
        self.log_message(message)
        print(message)

    def submit_sell_order(self, asset):
        # Create order
        order = self.create_order(
            asset,
            1,
            "sell_to_close",
        )
    
        # Submit order
        self.submit_order(order)

        # Log a message
        message = (f"Sold {order.quantity} of {asset}")
        self.log_message(message)
        print(message)        

    def before_market_closes(self):
        self.sell_all()
        self.all_buy_signals = dict()

    def on_abrupt_closing(self):
        self.sell_all()

    # =============Helper methods====================
    def create_trading_pair(self, symbol):
        # Add/update trading pair to self.trading_pairs
        self.trading_pairs[self.create_asset(symbol, asset_type="stock")] = {
            "call": None,
            "put": None,
            "chains": None,
            "expirations": None,
            "strike_lows": None,
            "strike_highs": None,
            "buy_call_strike": None,
            "buy_put_strike": None,
            "expiration_date": None,
            "open_price": None,
            "close_price": None,
            "price_underlying": None,
            "price_call": None,
            "price_put": None,
            "open_price_call": None,
            "open_price_put": None,
            "trade_created_time": None,
            "call_order": None,
            "put_order": None,
            "status": 0,      
            "puts": None,
            "calls": None,
            "open_puts": None,
            "open_calls": None,
            "last_calls": None,
            "last_puts": None,
        }

    def asset_cycle(self, assets):
        # Used to cycle through the assets for investing, prevents starting
        # at the beginning of the asset list on each iteration.
        for asset in cycle(assets):
            yield asset

    def call_put_strike(self, last_price, symbol, expiration_date):
        """Returns strikes for pair."""

        buy_call_strike = 0
        buy_put_strike = 0


        asset = Asset(symbol=symbol, asset_type="option", expiration=expiration_date, right="call", multiplier=100)

        asset = self.create_asset(
            symbol,
            asset_type="option",
            expiration=expiration_date,
            right="call",
            multiplier=100,
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
  
        from lumibot.brokers import InteractiveBrokers
        from lumibot.traders import Trader

        trader = Trader()
        broker = Alpaca(ALPACA_CONFIG)
        strategy = SimpleOptionStrategy(name="Simple Options Strategy", budget=100000, broker=broker, symbol="SPY")
        
        trader.add_strategy(strategy)
        strategy_executors = trader.run_all()

    else:
        from lumibot.backtesting import PolygonDataBacktesting

        # Backtest this strategy
        backtesting_start = datetime(2024, 7, 1)
        backtesting_end = datetime(2024, 7, 30)


        SimpleOptionStrategy.backtest(
            PolygonDataBacktesting,
            backtesting_start,
            backtesting_end,
            benchmark_asset="SPY",
            polygon_api_key="NrThd7hnOg9g9mKjTiqHuKdmJ9rQ7ZZQ",
        )