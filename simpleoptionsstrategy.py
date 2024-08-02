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
        self.sleeptime = "60m"
        self.minutes_before_closing = 30
        self.total_trades = 0
        self.max_trades = 4
        self.max_days_expiry = 15
        self.days_to_earnings_min = 100  # 15
        self.exchange = "SMART"
        self.strike_width = 1
        self.use_all_strikes = False
        self.trade_0dte = False

        self.trade_calls = True
        self.trade_puts = True

        self.all_open_orders = dict()        

        # Stock expected to move.
        #self.symbols_universe = ['AAPL', 'MSFT', 'AMD', 'META', 'NVDA', 'SPY', 'QQQ', 'TSLA', 'NFLX']
        #self.symbols_universe = ['AAPL', 'MSFT']
        #self.symbols_universe = ['AAPL']
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
        
        day_index = 5
        if self.trade_0dte == False:
            day_index = 4
        next_friday = today + timedelta(day_index-today.weekday())      
        
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

                upper_strike_price = values[len(values)-1]
                lower_strike_price = values[0]
                
                if not self.use_all_strikes:
                    strike_price = 0
                    previous_strike_price = 0
                    strike_index = 0
                    atm_index = 0
                    strike_price = 0
                    difference = 0
                    last_difference = 100000
                    for strike in values:

                        strike_price = round(strike)
                        difference = abs(strike_price - options["underlying_price"])
                        if difference > last_difference:                            
                            atm_index = strike_index
                            break

                        last_difference = difference
                        
                        #if previous_strike_price <= round(options["underlying_price"]) and strike_price > round(options["underlying_price"]):
                        #    atm_index = strike_index
                        #    break
                        #previous_strike_price = strike
                        strike_index += 1

                    #10 strikes - 5 above and 5 below
                    upper_strike_price = values[atm_index+width]
                    lower_strike_price = values[atm_index-width]
                    
                assets = list()
                for strike_price_value in values:                       
                    if strike_price_value >= lower_strike_price and strike_price_value <= upper_strike_price:

                        asset = Asset(asset.symbol, expiration=datetime.strptime(index, '%Y-%m-%d').date(), strike=str(strike_price_value), asset_type="option", right=right, multiplier=multiplier)
                        assets.append(asset)

                results = self.get_last_prices(assets)
                if results is not None:
                    return results
                    
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

        for asset, options in self.trading_pairs.items():      
            try:
                options["chains"] = self.get_chains(asset)
            except Exception as e:
                logging.info(f"Error: {e}")

            try:               
                last_price = self.get_last_price(asset)
                options["open_price"] = last_price
                assert last_price != 0
            except:
                logging.warning(f"Unable to get price data for {asset.symbol}.")
                options["open_price"] = 0                      
  
            # Get the Open prices for all 
            if self.trade_calls:
                calls = self.get_options_prices(asset, options, "call", self.strike_width, friday_date)
                if calls is not None:
                    options["open_calls"] = calls                                        

            if self.trade_puts:        
                puts = self.get_options_prices(asset, options, "put", self.strike_width, friday_date)
                if puts is not None:
                    options["open_puts"] = puts                                        
    
    def get_buy_signals(self, open_calls, calls, symbol, right):

        strike_signals = dict()
        missing_open_calls = dict()
        
        for call_index, call_value in calls.items():            
            try:
                for call, strike_value in calls.items():
                    try:
                        open_strike_value = open_calls[call]
                        if strike_value > open_strike_value:
                            strike_signals[call] = strike_value - open_strike_value

                        pass
                    except:
                        missing_open_calls[call] = call
                        
            except:
                pass

        return strike_signals

    def get_sell_signals(self, open_calls, calls, symbol, right, all_open_orders):
        differences = dict()
        difference = False

        strike_signals = dict()
        missing_open_calls = dict()
        
        try:                
            for call, strike_value in calls.items():
                try:

                    try:
                        open_strike_value = open_calls[call]
                        # Stop Loss
                        if strike_value < open_strike_value:
                            strike_signals[call] = strike_value - open_strike_value
                        # Take profit
                        elif strike_value > (open_strike_value*2):
                            strike_signals[call] = strike_value - open_strike_value
                        pass

                    except:
                        break

                except:
                    missing_open_calls[call] = call

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
  
        try: 
            for asset, options in self.trading_pairs.items():
                # See if we've got new highs
                if self.trade_calls:
                    calls = self.get_options_prices(asset, options, "call", self.strike_width, friday_date)
                    
                    if (options["last_calls"] == None):
                        options["last_calls"] = options["open_calls"]
                    buy_call_signals = self.get_buy_signals(options["open_calls"], calls, asset.symbol, "C")
                    sell_call_signals = self.get_sell_signals(options["open_calls"], calls, asset.symbol, "C", self.all_open_orders)
                    
                    # Save the old set of calls
                    options["last_calls"] = calls                                

                if self.trade_puts:            
                    puts = self.get_options_prices(asset, options, "put", self.strike_width, friday_date)
                    if (options["last_puts"] == None):
                        options["last_puts"] = options["open_puts"]
                    buy_put_signals = self.get_buy_signals(options["open_puts"], puts, asset.symbol, "P")
                    sell_put_signals = self.get_sell_signals(options["open_puts"], puts, asset.symbol, "P", self.all_open_orders)

                    # Save the old set of puts
                    options["last_puts"] = puts

                if len(sell_call_signals) > 0 or len(sell_put_signals) >0:
                    pass

                # We can't buy and sell at the same time (though sometimes this can happen with TP/SL )
                #for index, value in buy_call_signals.items():
                #    if sell_call_signals.get(str(index)):
                #        del sell_call_signals[str(index)]


                ###
                # Sell 
                ###
                #for right in ["call", "put"]:
                if not self.first_iteration and len(self.all_open_orders) < 0:
                    if self.trade_calls:
                        for index, signal in sell_call_signals.items():
                            #try:
                            #    _asset = self.all_open_orders[index]
                            #    
                            #    # Submit order
                            #    self.submit_sell_order(_asset)
                            #    del self.all_open_orders[index]   
                            #except:
                            #    pass

                            if self.all_open_orders.get(str(index)):
                                _asset, request = self.submit_sell_order(index)
                                #self.all_pending_orders[str(index)] = _asset
                                #del self.all_open_orders[str(index)]   


                    if self.trade_puts:
                        for index, signal in sell_put_signals.items():
                            #try:
                            #    _asset = self.all_open_orders[index]                        
                                
                            #    # Submit order
                            #    self.submit_sell_order(_asset)
                            #    del self.all_open_orders[index]                                                
                            #except:
                            #    pass

                            if self.all_open_orders.get(str(index)):
                                _asset, request = self.submit_sell_order(index)
                                #self.all_pending_orders[str(index)] = _asset
                                #del self.all_open_orders[str(index)]                                                
                ###
                # Buy 
                ###
                #for right in ["call", "put"]:
                if self.trade_calls:
                    for index, signal in buy_call_signals.items():
                        #try:
                        #    buy_signal = self.all_open_orders[index]                        
                        #except:
                        #    _asset = self.submit_buy_order(asset.symbol, "option", next(iter(options["last_calls"])).expiration, index.strike, "call")
                        #    self.all_open_orders[index] = _asset

                        if not self.all_open_orders.get(index):
                            _asset = self.submit_buy_order(asset.symbol, "option", next(iter(options["last_calls"])).expiration, index.strike, "call", price=signal)
                            self.all_open_orders[str(index)] = _asset
                            #self.all_pending_orders[str(index)] = _asset



                if self.trade_puts:
                    for index, signal in buy_put_signals.items():
                        #try:
                        #    buy_signal = self.all_open_orders[index]                        
                        #except:
                        #    _asset = self.submit_buy_order(asset.symbol, "option", next(iter(options["last_puts"])).expiration, index.strike, "put")                        
                        #    self.all_open_orders[index] = _asset

                        if not self.all_open_orders.get(index):
                            _asset = self.submit_buy_order(asset.symbol, "option", next(iter(options["last_puts"])).expiration, index.strike, "put", price=signal)                        
                            request = self.all_open_orders[str(index)] = _asset
                            #self.all_pending_orders[str(index)] = request

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

            
        except:
            pass
        # self.await_market_to_close()

    def submit_buy_order(self, asset, asset_type, expiry, strike, right, price):

        try:
            # Create options asset
            _asset = Asset(
                symbol=asset,
                asset_type=asset_type,
                expiration=expiry,
                strike=strike,
                right=right,
            )

            # Create order
            order = self.create_order(
                asset,
                1,
                "buy_to_open",
                take_profit_price=price * 0.9,
                stop_loss_price=price * 2,                                
            )
        
            # Submit order
            request = self.submit_order(order)

            # Log a message
            message = (f"Bought {order.quantity} of {asset} at {strike} {expiry} price {price} ")
            self.log_message(message)
            print(message)

            return _asset
        except Exception as e:
            print(f"Error: {e}")

    def submit_sell_order(self, asset):
        # Create order

        try:
            _asset = Asset(
                symbol=asset.symbol,
                asset_type=asset.asset_type,
                expiration=asset.expiration,
                strike=asset.strike,
                right=asset.right,
            )

            order = self.create_order(
                asset,
                1,
                "sell_to_close",
            )
        
            # Submit order
            request = self.submit_order(order)

            # Log a message
            message = (f"Sold {order.quantity} of {asset}")
            self.log_message(message)
            print(message)        

            return _asset
        except Exception as e:
            print(f"Error: {e}")

    def before_market_closes(self):

        print(
            f"**** Market closing ****\n"
            f"Time: {self.get_datetime()} " 
            f"Cash: {self.cash}, Value: {self.portfolio_value}  "
            f"*******  END ELAPSED TIME  "
            f"{(time.time() - self.time_start):5.0f}   "
            f"*******"
        )        

        self.sell_all()        

        self.all_open_orders = dict()

    def on_abrupt_closing(self):
        self.sell_all()

    def on_new_order(self, order):
        self.log_message("%r is currently being processed by the broker" % order)

    def on_partially_filled_order(self, order, price, quantity, multiplier):
        missing = order.quantity - quantity
        self.log_message(f"{quantity} has been filled")
        self.log_message(f"{quantity} waiting for the remaining {missing}")

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

    def on_filled_order(self, position, order, price, quantity, multiplier):
        if order.side == "sell":
            self.log_message(f"{quantity} shares of {order.symbol} has been sold at {price}$")
        elif order.side == "buy":
            self.log_message(f"{quantity} shares of {order.symbol} has been bought at {price}$")

        self.log_message(f"Currently holding {position.quantity} of {position.symbol}")    

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