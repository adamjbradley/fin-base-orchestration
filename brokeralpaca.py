import logging

from kafkaproducer import KafkaProducer
from kafkaconsumerthreaded import KafkaConsumer

from tradingstrategy import Strategy

import datetime
import threading

import alpaca_trade_api as tradeapi
import time

import alpaca
from alpaca.data.live.option import *
from alpaca.data.historical.option import *
from alpaca.data.requests import *
from alpaca.data.timeframe import *
from alpaca.trading.client import *
from alpaca.trading.stream import *
from alpaca.trading.requests import *
from alpaca.trading.enums import *
from alpaca.common.exceptions import APIError

import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytz

import random

# Stock WS streaming
from alpaca_trade_api.stream import Stream
from alpaca_trade_api.common import URL

from enum import Enum


# Below are the variables for development this documents
# Please do not change these variables

trade_api_url = None
trade_api_wss = None
data_api_url = None
option_stream_data_wss = None


# Please change the following to your own PAPER api key and secret
# You can get them from https://alpaca.markets/

# Alpaca WebSocket Client
# From https://github.com/alpacahq/alpaca-py/blob/master/examples/options-trading-basic.ipynb


class ChaosMonkey:
    def __init__(self):        
        self.Enabled = False
        self.OpeningTime = False
        self.StockPicker = True

    def coinflip(self):
        if self.Enabled == True:
          return bool(random.getrandbits(1))

    def lesslikely(self):
        if self.Enabled == True:
          if int(str(datetime.now().second)) < 5:
            return True

#
# Market State Classes
#
class MarketState(Enum):
    Open = 1
    AboutToOpen = 2
    JustOpened = 3
    AboutToClose = 4
    Closed = 5
    JustClosed = 6
    ClosedForWeekend = 7
    ClosedForHoliday = 8

#
# Contract/Trade Classes
#
class Contract:
    def __init__(self, contract):        
        self.expiration_date = contract.expiration_date
        self.symbol = contract.symbol
        self.strike_price = contract.strike_price        
        self.raw_contract = contract
        self.open_price = None
        self.open_price_date = None
        self.close_price = contract.close_price

class Contracts:
    def __init__(self):
      pass

    def addContractsFromList(self, new_contracts):

      # Chaos Monkey!
      for contract in new_contracts:

        self.addContracts(self, new_contracts)

        if chaos_monkey:
          if bool(random.getrandbits(1)):
            #if bool(random.getrandbits(1)):
            #  if bool(random.getrandbits(1)):
                self.newcontracts[contract.symbol] = Contract(contract)
        else:
          self.newcontracts[contract.symbol] = Contract(contract)   
    
      for key, value in self.allcontracts.items():
          if key not in self.newcontracts:  # if key not match
              logging.debug("key: {}, missing values: not in newcontracts - delete contract".format(key))
              self.contractstoberemoved[value.symbol] = Contract(value)

      for key, value in self.newcontracts.items():
          if key not in self.allcontracts:
              logging.debug("key: {}, not in allcontracts - new contract".format(key))              
              self.contractstobeadded[value.symbol] = Contract(value)
              

      self.allcontracts = {**self.allcontracts, **self.newcontracts}

      logging.info("Existing contract not found in new contracts - deleting {} contract(s)".format(len(self.contractstoberemoved)))
      logging.info("New contract not found in contracts - adding {} contract(s)".format(len(self.contractstobeadded)))

      logging.info("{} all contracts ".format(len(self.allcontracts)))
      
      self.newcontracts = {}

    def addContractsFromDict(self, new_contracts):
      
      # Chaos Monkey!
      for key, value in new_contracts.items():

        if self.chaos_monkey.lesslikely():
            self.newcontracts[key] = Contract(value)  
        else:
           self.newcontracts[key] = Contract(value)        
    
      for key, value in self.allcontracts.items():
          if key not in self.newcontracts:  # if key not match
              logging.debug("key: {}, missing values: not in newcontracts - delete contract".format(key))
              self.contractstoberemoved[value.symbol] = Contract(value)

      for key, value in self.newcontracts.items():
          if key not in self.allcontracts:
              logging.debug("key: {}, not in allcontracts - new contract".format(key))              
              self.contractstobeadded[value.symbol] = Contract(value)              

      self.allcontracts = {**self.allcontracts, **self.newcontracts}

      logging.info("Existing contract not found in new contracts - deleting {} contract(s)".format(len(self.contractstoberemoved)))
      logging.info("New contract not found in contracts - adding {} contract(s)".format(len(self.contractstobeadded)))

      logging.info("{} all contracts ".format(len(self.allcontracts)))
      
      #self.newcontracts = {}

class Trade:
    def __init__(self, name, time):        
        self.last_update_time
        self.symbol = name
        self.buy_price = 0.0
        self.sl = 0.0

class Trades:
    def __init__(self):
        self.trades = []

class Broker:
  def __init__(self, api_key, secret_key, paper, producer, consumer, log):

    # Logging and debugging
    self.log = log
    self.chaos_monkey = ChaosMonkey()

    # Broker
    self.producer = producer
    self.consumer = consumer

    # Connection
    self.api_key = api_key
    self.secret_key = secret_key

    # Setup client
    self.trade_client = TradingClient(api_key=api_key, secret_key=secret_key, paper=paper, url_override=trade_api_url)

    # We're monitoring these...
    self.underlyingsymbols = ["AAPL","TSLA","MSFT","NVDA","SPY", "NVDA"]

    # Initialize contracts handling
    self.allcontracts = {}
    self.newcontracts = {}
    self.contractstoberemoved = {}
    self.contractstobeadded = {}

    # Market state
    self.lastMarketState = MarketState.Closed
    self.isMarketOpen = False

    # For testing
    self.SampleContract = "SPY240716P00515000"

    self.updatedOptionsContracts = False
    self.newOptionsContracts = False
    self.removedOptionsContracts = False

    self.isOptionsConnected = False
    self.isStockConnected = False
   
    self.options_conn = OptionDataStream(self.api_key, self.secret_key, url_override = option_stream_data_wss)
    
    feed = 'iex'  # <- replace to SIP if you have PRO subscription
    self.stocks_conn = Stream(self.api_key,
                  self.secret_key,
                  base_url=URL('https://paper-api.alpaca.markets'),
                  data_feed=feed)
    
  # Wait for market to open.
  def awaitMarketOpen(self):
    while True:

      self.isMarketOpen = self.trade_client.get_clock().is_open
      
      # Having his fun!
      if self.chaos_monkey.coinflip():
        self.isMarketOpen = True
        self.marketState = MarketState.JustOpened
    
      while(not self.isMarketOpen):
        clock = self.trade_client.get_clock()
        openingTime = clock.next_open.replace(tzinfo=pytz.UTC).timestamp()
        currTime = clock.timestamp.replace(tzinfo=pytz.UTC).timestamp()
        timeToOpen = int((openingTime - currTime) / 60)           
        log.info(str(timeToOpen) + " minutes til market open.")

        if (timeToOpen <= 1):
           time.sleep(1)
        else:
           time.sleep(60)
        
        self.isMarketOpen = self.trade_client.get_clock().is_open

        # Having his fun!
        if self.chaos_monkey.coinflip():
          self.isMarketOpen = True
          self.lastMarketState = MarketState.Closed

      # Market is open, handle market state transitions
      if not self.isMarketOpen:
        if self.lastMarketState == MarketState.Open:
          self.lastMarketState = MarketState.JustClosed
          self.marketState = MarketState.Closed
        elif self.lastMarketState == MarketState.JustClosed:
          self.lastMarketState = MarketState.Closed

      if self.isMarketOpen:
        if self.lastMarketState == MarketState.Closed:
          self.lastMarketState = MarketState.JustOpened
          self.marketState = MarketState.Open
        elif self.lastMarketState == MarketState.JustOpened:
          self.lastMarketState = MarketState.Open

      time.sleep(60)

  def thisFriday(self):
      today = datetime.today() 
      next_friday = today + timedelta(5-today.weekday())      
      return next_friday
  
  # getOptionsContracts returns contracts, we want to look at each of them by interrogating contract.symbol. Sample contact SPY240716P00515000
  def getOptionsContracts(self, type, underlying_symbols, expiry_date, strike_price_lte, strike_price_gte, limit):
    # specify expiration date range
    now = datetime.now(tz = ZoneInfo("America/New_York"))
    day1 = now + timedelta(days = 0)
    #day60 = now + timedelta(days = 60)

    logging.info("getOptionsContracts: entering")

    #Want to aggregate them here
    newcontracts = {}

    req = GetOptionContractsRequest(
        underlying_symbols = underlying_symbols,                     # specify underlying symbols
        status = AssetStatus.ACTIVE,                                 # specify asset status: active (default)
        expiration_date = None,                                      # specify expiration date (specified date + 1 day range)
        expiration_date_gte = day1.date(),                           # we can pass date object
        #expiration_date_lte = day60.strftime(format = "%Y-%m-%d"),   # or string
        expiration_date_lte = expiry_date.strftime(format = "%Y-%m-%d"),   # or string
        root_symbol = None,                                          # specify root symbol
        type = type,                                                 # specify option type: put
        style = ExerciseStyle.AMERICAN,                              # specify option style: american
        strike_price_gte = strike_price_gte,                                     # specify strike price range
        strike_price_lte = strike_price_lte,                                     # specify strike price range
        limit = limit,                                                 # specify limit
        page_token = None,                                           # specify page
    )
    res = self.trade_client.get_option_contracts(req)
    
    for contract in res.option_contracts:
      newcontracts[contract.symbol] = contract

    #Contracts.addContractsFromList(self, new_contracts=res.option_contracts)

    # continue to fetch option contracts if there is next_page_token in response
    if res.next_page_token is not None:
        req = GetOptionContractsRequest(
            underlying_symbols = underlying_symbols,               # specify underlying symbols
            status = AssetStatus.ACTIVE,                           # specify asset status: active (default)
            expiration_date = None,                                # specify expiration date (specified date + 1 day range)
            expiration_date_gte = None,                            # we can pass date object
            expiration_date_lte = None,                            # or string (YYYY-MM-DD)
            root_symbol = None,                                    # specify root symbol
            type = type,                                           # specify option type (ContractType.CALL or ContractType.PUT)
            style = None,                                          # specify option style (ContractStyle.AMERICAN or ContractStyle.EUROPEAN)
            strike_price_gte = strike_price_gte,                               # specify strike price range
            strike_price_lte = strike_price_lte,                               # specify strike price range
            limit = 1000,                                             # specify limit
            page_token = res.next_page_token,                      # specify page token
        )        
        res = self.trade_client.get_option_contracts(req)

        for contract in res.option_contracts:
          newcontracts[contract.symbol] = contract

        #Contracts.addContractsFromList(self, new_contracts=res.option_contracts)

    logging.info("getOptionsContracts: leaving")
    return newcontracts

  def getNewOptionsContracts(self):

    while True:
      if self.isMarketOpen:

        #TODO Implement strategy here
        # Expire this week
        thisFriday = self.thisFriday()

        # Affordable!
        call_option_contracts = self.getOptionsContracts("call", self.underlying_symbols, thisFriday, "100", "0", 5)    
        put_option_contracts = self.getOptionsContracts("put", self.underlying_symbols, thisFriday, "100", "0", 5)
        all_option_contracts = {**call_option_contracts, **put_option_contracts}

        # List of revised contracts
        Contracts.addContractsFromDict(self, new_contracts=all_option_contracts)

        # Provide Data to satisfy various strategies - only give them what they need
        if self.marketState == MarketState.JustOpened:
          for key, value in all_option_contracts.items():
            self.allcontracts[key].open_price = all_option_contracts[key].close_price
            self.allcontracts[key].open_price_time = datetime.now
                    
        # Strategy #1 - Top 10 contracts based on % gain from open      
        if len(self.contractstoberemoved) > 0:
          if self.removeOptionsContracts(self.contractstoberemoved):
            # Handle them
            self.contractstoberemoved = {}
        if len(self.contractstobeadded) > 0:
          if self.addOptionsContracts(self.contractstobeadded):
            # Handle
            self.contractstobeadded = {}

      time.sleep(30)

#region Options handling

  # Retrieve all Options contracts for the given underlying stocks ITM
  async def option_quotes_data_stream_handler(self, data):
      self.producer.publish(data)
      logging.info('Options stream quotes update', data)

  async def option_trades_data_stream_handler(self, data):
      self.producer.publish(data)
      logging.info('Options stream trades update', data)

  def start_option_data_stream(self):

      if not self.isMarketOpen:
        return
       
      try:
        logging.info("Starting WSS for options")        
        self.options_conn.run()

        self.isOptionsConnected = True

      except Exception as e:
        self.isOptionsConnected = False
        logging.info("You got an exception: {} during execution. continue "
            "execution.".format(e))
        # let the execution continue
        pass

  def stop_option_data_stream(self):
      if not self.isMarketOpen:
        return False

      try:
        if (self.options_conn._running):
          logging.info("Stopping WSS for options")
          self.options_conn.stop()
      except Exception as e:
          logging.info("You got an exception: {} during execution. continue "
              "execution.".format(e))
          # let the execution continue
          pass
      
      return False

  def removeOptionsContracts(self, contracts):

      if not self.isMarketOpen:
        return False

      symbols = [
          contracts
      ]

      try:
        if (self.options_conn._running):
          self.options_conn.unsubscribe_quotes(*symbols)
          self.options_conn.unsubscribe_trades(*symbols)
          return True
      except Exception as e:
          logging.info("You got an exception: {} during execution. continue "
              "execution.".format(e))
          # let the execution continue          
          pass
      
      return False

  def addOptionsContracts(self, contracts):

      if not self.isMarketOpen:
        return False
      
      symbols = [
          contracts
      ]

      try:
        if (self.options_conn._running):
          self.options_conn.subscribe_quotes(self.option_quotes_data_stream_handler, *symbols)
          self.options_conn.subscribe_trades(self.option_trades_data_stream_handler, *symbols)        
      except Exception as e:
            logging.info("You got an exception: {} during execution. continue "
                "execution.".format(e))
            # let the execution continue      
            pass
      
      return False

#endregion

#region Stock Handling

# Retrieve all Stock updates
  async def stock_data_stream_handler(self, data):
      self.producer.publish(data)
      logging.info("Starting WSS for stocks")
      logging.info('Stocks stream update', data)

  def start_stock_data_stream(self):

      if not self.isMarketOpen:
        return

      try:
        logging.info("Starting WSS for stocks")
        self.stocks_conn.subscribe_quotes(self.stock_data_stream_handler, 'AAPL')
        self.stocks_conn.run()

        self.isStockConnected = True

      except Exception as e:
        self.isStockConnected = False #TODO Should be an enum
        logging.info("You got an exception: {} during execution. continue "
            "execution.".format(e))
        # let the execution continue
        pass

  def stop_stock_data_stream(self):    

      if not self.isMarketOpen:
        return

      try:
        log.info("Stopping WSS for stocks")
        if (self.stocks_conn._running):    
          self.stocks_conn.stop()
        isStockConnected = False
      except Exception as e:        
        logging.info("You got an exception: {} during execution. continue "
            "execution.".format(e))
        # let the execution continue
        pass

  def removeStocks(self, stocks):

      if not self.isMarketOpen:
        return

      symbols = [
          stocks
      ]

      try:
        if (self.stocks_conn._running):
          self.stocks_conn.unsubscribe_quotes(*symbols)
      except Exception as e:
          logging.info("You got an exception: {} during execution. continue "
              "execution.".format(e))
          # let the execution continue
          pass

  def addStocks(self, stocks):

      if not self.isMarketOpen:
        return

      symbols = [
          stocks
      ]

      try:
        if (self.stocks_conn._running):
          self.stocks_conn.subscribe_quotes(self.stock_data_stream_handler, symbols)
      except Exception as e:
            logging.info("You got an exception: {} during execution. continue "
                "execution.".format(e))
            # let the execution continue
            pass

#endregion