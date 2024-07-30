import logging
import time
from itertools import cycle

import pandas as pd

from lumibot.entities import Asset, TradingFee
from lumibot.strategies.strategy import Strategy
from lumibot.traders import Trader

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

        # Stock expected to move.
        #self.symbols_universe = ['META', 'AAPL', 'SPY', 'QQQ', 'TSLA','NVDA', 'MSFT', 'AMD', 'NFLX']
        self.symbols_universe = ['META', 'MSFT', 'AMD']

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
    

    def get_options_prices(self, asset, options, right):
        for asset, options in self.trading_pairs.items():

            print(
            f"**** Before starting trading ****\n"
            f"Time: {self.get_datetime()} "
            f"Retrieving Data for : {asset.symbol} "
            f"Cash: {self.cash}, Value: {self.portfolio_value}  "
            f"*******  END ELAPSED TIME  "
            f"{(time.time() - self.time_start):5.0f}   "
            f"*******"
            )

            #try:
            #    if not options["chains"]:
            #        options["chains"] = self.get_chains(asset)
            #except Exception as e:
            #    logging.info(f"Error: {e}")
            #    continue

            try:               
                last_price = self.get_last_price(asset)
                options["price_underlying"] = last_price
                assert last_price != 0
            except:
                logging.warning(f"Unable to get price data for {asset.symbol}.")
                options["price_underlying"] = 0
                continue      

            # We're only interested in options chains that expire before this coming Friday
            multiplier = self.get_multiplier(options["chains"])
            friday_date = self.thisFriday(self.get_datetime()).date()

            strike_expiration = {}
            strike_greeks_calls = {}
            for index, values in options['chains']['Chains']['CALL'].items():
                            
                if datetime.strptime(index, '%Y-%m-%d').date() >= friday_date:
                    break
                else:

                    for strike in values:

                        strike_price = round(strike)      
                        upper = options["price_underlying"] + (options["price_underlying"] * 0.05)
                        lower = options["price_underlying"] - (options["price_underlying"] * 0.05)

                        if strike_price > lower and strike_price < upper:                        
                            call_asset = Asset(asset.symbol, expiration=datetime.strptime(index, '%Y-%m-%d').date(), strike=strike_price, asset_type="option", right="call", multiplier=multiplier)
                            #calls = self.get_greeks(call_asset, query_greeks=True)                       

                            calls = self.get_last_price(call_asset)
                            strike_greeks_calls[strike_price] = calls

                            #if calls["option_price"] is not None:
                            #    calls["right"] = "call"
                            #    strike_greeks_calls[strike_price] = calls
                        
                        strike_expiration[index] = strike_greeks_calls

            options["calls"] = strike_expiration


            strike_expiration = {}
            strike_greeks_puts = {}
            for index, values in options['chains']['Chains']['PUT'].items():            
                if datetime.strptime(index, '%Y-%m-%d').date() >= friday_date:
                    break
                else:

                    for strike in values:

                        strike_price = round(strike)              
                        upper = options["price_underlying"] + (options["price_underlying"] * 0.05)
                        lower = options["price_underlying"] - (options["price_underlying"] * 0.05)

                        if strike_price > lower and strike_price < upper:                       

                            put_asset = Asset(asset.symbol, expiration=datetime.strptime(index, '%Y-%m-%d').date(), strike=strike_price, asset_type="option", right="put", multiplier=multiplier)
                            #puts = self.get_greeks(put_asset, query_greeks=True)

                            puts = self.get_last_price(put_asset)
                            strike_greeks_calls[strike_price] = puts

                            #if puts["option_price"] is not None:
                            #    puts["right"] = "put"
                            #    strike_greeks_puts[strike_price] = puts
                        
                        strike_expiration[index] = strike_greeks_puts

            options["puts"] = strike_expiration

            if not options["buy_call_strike"] or not options["buy_put_strike"]:
                logging.info(f"No options data for {asset.symbol}")
                continue





    def before_starting_trading(self):
        """Create the option assets object for each underlying. """
        self.asset_gen = self.asset_cycle(self.trading_pairs.keys())


        for asset, options in self.trading_pairs.items():


            print(
            f"**** Before starting trading ****\n"
            f"Time: {self.get_datetime()} "
            f"Retrieving Data for : {asset.symbol} "
            f"Cash: {self.cash}, Value: {self.portfolio_value}  "
            f"*******  END ELAPSED TIME  "
            f"{(time.time() - self.time_start):5.0f}   "
            f"*******"
            )

            try:
                if not options["chains"]:
                    options["chains"] = self.get_chains(asset)
            except Exception as e:
                logging.info(f"Error: {e}")
                continue

            try:               
                last_price = self.get_last_price(asset)
                options["price_underlying"] = last_price
                assert last_price != 0
            except:
                logging.warning(f"Unable to get price data for {asset.symbol}.")
                options["price_underlying"] = 0
                continue      

            calls = self.get_options_prices(asset, options, "call")
            puts = self.get_options_prices(asset, options, "put")

            dothis = False
            if dothis:
                # We're only interested in options chains that expire before this coming Friday
                multiplier = self.get_multiplier(options["chains"])
                friday_date = self.thisFriday(self.get_datetime()).date()

                strike_expiration = {}
                strike_greeks_calls = {}
                for index, values in options['chains']['Chains']['CALL'].items():
                                
                    if datetime.strptime(index, '%Y-%m-%d').date() >= friday_date:
                        break
                    else:

                        for strike in values:

                            strike_price = round(strike)      
                            upper = options["price_underlying"] + (options["price_underlying"] * 0.05)
                            lower = options["price_underlying"] - (options["price_underlying"] * 0.05)

                            if strike_price > lower and strike_price < upper:                        
                                call_asset = Asset(asset.symbol, expiration=datetime.strptime(index, '%Y-%m-%d').date(), strike=strike_price, asset_type="option", right="call", multiplier=multiplier)
                                #calls = self.get_greeks(call_asset, query_greeks=True)                       

                                calls = self.get_last_price(call_asset)
                                strike_greeks_calls[strike_price] = calls

                                #if calls["option_price"] is not None:
                                #    calls["right"] = "call"
                                #    strike_greeks_calls[strike_price] = calls
                            
                            strike_expiration[index] = strike_greeks_calls

                options["calls"] = strike_expiration


                strike_expiration = {}
                strike_greeks_puts = {}
                for index, values in options['chains']['Chains']['PUT'].items():            
                    if datetime.strptime(index, '%Y-%m-%d').date() >= friday_date:
                        break
                    else:

                        for strike in values:

                            strike_price = round(strike)              
                            upper = options["price_underlying"] + (options["price_underlying"] * 0.05)
                            lower = options["price_underlying"] - (options["price_underlying"] * 0.05)

                            if strike_price > lower and strike_price < upper:                       

                                put_asset = Asset(asset.symbol, expiration=datetime.strptime(index, '%Y-%m-%d').date(), strike=strike_price, asset_type="option", right="put", multiplier=multiplier)
                                #puts = self.get_greeks(put_asset, query_greeks=True)

                                puts = self.get_last_price(put_asset)
                                strike_greeks_calls[strike_price] = puts

                                #if puts["option_price"] is not None:
                                #    puts["right"] = "put"
                                #    strike_greeks_puts[strike_price] = puts
                            
                            strike_expiration[index] = strike_greeks_puts

                options["puts"] = strike_expiration

            if not options["buy_call_strike"] or not options["buy_put_strike"]:
                logging.info(f"No options data for {asset.symbol}")
                continue


    def on_trading_iteration(self):
        value = self.portfolio_value
        cash = self.cash
        positions = self.get_tracked_positions()
        filled_assets = [p.asset for p in positions]
        trade_cash = self.portfolio_value / (self.max_trades * 2)

        # Await market to open (on_trading_iteration will stop running until the market opens)
        enableSell = False
        if enableSell:
            # Sell positions:
            for asset, options in self.trading_pairs.items():

                if self.minutes_before_opening == 0:
                    if options["call"] in filled_assets:
                        options["open_price_call"] = options["call"].strike
                    if options["put"] in filled_assets:
                        options["open_price_put"] = options["put"].strike
                    
                    
                if (
                    options["call"] not in filled_assets
                    and options["put"] not in filled_assets
                ):
                    continue

                if options["status"] > 1:
                    continue

                last_price = self.get_last_price(asset)
                if last_price == 0:
                    continue

                # The sell signal will be the maximum percent movement of original price
                # away from strike, greater than the take profit threshold.
                price_move = max(
                    [
                        (last_price - options["call"].strike),
                        (options["put"].strike - last_price),
                    ]
                )

                if price_move / options["price_underlying"] > self.take_profit_threshold:
                    # Create order
                    order = self.create_order(
                        asset,
                        10,
                        "buy_to_open",
                    )

                    # Submit order
                    #self.submit_order(order)

                    # Log a message
                    self.log_message(f"Bought {order.quantity} of {asset}")

                    options["status"] = 2
                    self.total_trades -= 1

        enableBuy = False
        if enableBuy:
            # Create positions:
            if self.total_trades >= self.max_trades:
                return

            for _ in range(len(self.trading_pairs.keys())):
                if self.total_trades >= self.max_trades:
                    break

                asset = next(self.asset_gen)
                options = self.trading_pairs[asset]
                if options["status"] > 0:
                    continue

                # Check for symbol in positions.
                if len([p.symbol for p in positions if p.symbol == asset.symbol]) > 0:
                    continue
                # Check if options already traded.
                if options["call"] in filled_assets or options["put"] in filled_assets:
                    continue

                # Get the latest prices for stock and options.
                try:
                    print(asset, options["call"], options["put"])
                    asset_prices = self.get_last_prices(
                        [asset, options["call"], options["put"]]
                    )
                    assert len(asset_prices) == 3
                except:
                    logging.info(f"Failed to get price data for {asset.symbol}")
                    continue

                options["price_underlying"] = asset_prices[asset]
                options["price_call"] = asset_prices[options["call"]]
                options["price_put"] = asset_prices[options["put"]]

                # Check to make sure date is not too close to earnings.
                print(f"Getting earnings date for {asset.symbol}")
                edate_df = Ticker(asset.symbol).calendar
                if edate_df is None:
                    print(
                        f"There was no calendar information for {asset.symbol} so it "
                        f"was not traded."
                    )
                    continue
                edate = edate_df.iloc[0, 0].date()
                current_date = datetime.datetime.now().date()
                days_to_earnings = (edate - current_date).days
                if days_to_earnings > self.days_to_earnings_min:
                    logging.info(
                        f"{asset.symbol} is too far from earnings at" f" {days_to_earnings}"
                    )
                    continue

                options["trade_created_time"] = datetime.datetime.now()

                quantity_call = int(
                    trade_cash / (options["price_call"] * options["call"].multiplier)
                )
                quantity_put = int(
                    trade_cash / (options["price_put"] * options["put"].multiplier)
                )

                # Check to see if the trade size it too big for cash available.
                if quantity_call == 0 or quantity_put == 0:
                    options["status"] = 2
                    continue

                # Buy call.
                options["call_order"] = self.create_order(
                    options["call"],
                    quantity_call,
                    "buy",
                    exchange=self.exchange
                )
                #self.submit_order(options["call_order"])

                # Buy put.
                options["put_order"] = self.create_order(
                    options["put"],
                    quantity_put,
                    "buy",
                    exchange=self.exchange
                )
                #self.submit_order(options["put_order"])

                self.total_trades += 1
                options["status"] = 1

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

    def before_market_closes(self):
        self.sell_all()
        self.trading_pairs = dict()

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
        from credentials import INTERACTIVE_BROKERS_CONFIG

        from lumibot.brokers import InteractiveBrokers
        from lumibot.traders import Trader

        trader = Trader()

        broker = InteractiveBrokers(INTERACTIVE_BROKERS_CONFIG)
        strategy = Strangle(broker=broker)

        trader.add_strategy(strategy)
        strategy_executors = trader.run_all()

    else:
        from lumibot.backtesting import PolygonDataBacktesting

        # Backtest this strategy
        backtesting_start = datetime(2024, 7, 25)
        backtesting_end = datetime(2024, 7, 29)


        SimpleOptionStrategy.backtest(
            PolygonDataBacktesting,
            backtesting_start,
            backtesting_end,
            benchmark_asset="SPY",
            polygon_api_key="NrThd7hnOg9g9mKjTiqHuKdmJ9rQ7ZZQ",
        )