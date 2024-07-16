import logging

import datetime
import threading

import alpaca_trade_api as tradeapi
import time
#from alpaca_trade_api.rest import TimeFrame
#import pandas as pd
#from datetime import datetime, timedelta

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

# Below are the variables for development this documents
# Please do not change these variables

trade_api_url = None
trade_api_wss = None
data_api_url = None
option_stream_data_wss = None
chaos_monkey = False

# Please change the following to your own PAPER api key and secret
# You can get them from https://alpaca.markets/

# Alpaca WebSocket Client
# From https://github.com/alpacahq/alpaca-py/blob/master/examples/options-trading-basic.ipynb

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
        self.close_price = contract.close_price

class Contracts:
    def __init__(self):
      pass

    def addContractsFromList(self, new_contracts):

      # Chaos Monkey!
      for contract in new_contracts:
        if chaos_monkey:
          if bool(random.getrandbits(1)):
             new_contracts.pop(random.randint(0, len(new_contracts)-1))
          else:
            self.newcontracts[contract.symbol] = Contract(contract)
        else:
          self.newcontracts[contract.symbol] = Contract(contract)
    
      for key, value in self.allcontracts.items():
          if key not in self.newcontracts:  # if key not match
              logging.debug("key: {}, missing values: not in newcontracts - delete contact".format(key))
              self.contractstoberemoved[value.symbol] = Contract(value)

      for key, value in self.newcontracts.items():
          if key not in self.allcontracts:
              logging.debug("key: {}, not in allcontracts - new contract".format(key))              
              self.contractstobeadded[value.symbol] = Contract(value)
              

      self.allcontracts = {**self.allcontracts, **self.newcontracts}

      logging.info("{} missing values: not in new contracts - delete contact".format(len(self.contractstoberemoved)))
      logging.info("{} missing values: not in all contracts - add contact".format(len(self.contractstobeadded)))

      logging.info("{} new contracts ".format(len(self.newcontracts)))
      logging.info("{} all contracts ".format(len(self.allcontracts)))


      
      self.newcontracts = {}

      if len(self.contractstoberemoved) > 0:
        self.removeOptionsContracts(key)
        # Handle them
        self.contractstoberemoved = {}
      if len(self.contractstobeadded) > 0:
         self.addOptionsContracts(key)
         # Handle
         self.contractstobeadded = {}


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
  def __init__(self, api_key, secret_key, paper):
    #self.alpaca = tradeapi.REST(api_key, secret_key, "https://paper-api.alpaca.markets", 'v2')

    logging.basicConfig(format='%(asctime)s  %(levelname)s %(message)s', level=logging.INFO)

    self.timeToClose = None
    self.api_key = api_key
    self.secret_key = secret_key

    # Setup client
    self.trade_client = TradingClient(api_key=api_key, secret_key=secret_key, paper=paper, url_override=trade_api_url)

    # Initialize contracts handling
    self.allcontracts = {}
    self.activecontracts = {}
    self.newcontracts = {}
    self.updatedcontracts = {}
    self.contractstoberemoved = {}
    self.contractstobeadded = {}

    self.isMarketOpen = False
    self.MarketJustOpened = False
    self.MarketJustClosed = False

    self.SampleContract = None

    self.updatedOptionsContracts = False
    self.newOptionsContracts = False
    self.removedOptionsContracts = False

    self.isConnected = False

    global conn
    conn = OptionDataStream(self.api_key, self.secret_key, url_override = option_stream_data_wss)

  def run(self):

    # Wait for market to open.
    print("Waiting for market to open...")
    tAMO = threading.Thread(target=self.awaitMarketOpen)
    tAMO.start()
    tAMO.join()
    print("Market opened.")

    while True:

      # Figure out when the market will close so we can prepare to sell beforehand.
      clock = self.alpaca.get_clock()
      closingTime = clock.next_close.replace(tzinfo=ZoneInfo.fromutc).timestamp()
      currTime = clock.timestamp.replace(tzinfo=datetime.timezone.utc).timestamp()
      self.timeToClose = closingTime - currTime

      if(self.timeToClose < (60 * 15)):
        # Close all positions when 15 minutes til market close.
        print("Market closing soon.  Closing positions.")

        # Run script again after market close for next trading day.
        print("Sleeping until market close (15 minutes).")
        time.sleep(60 * 15)
      else:
        time.sleep(5)

  # Wait for market to open.
  def awaitMarketOpen(self):
    self.isMarketOpen = self.trade_client.get_clock().is_open
    while(not self.isMarketOpen):
      clock = self.trade_client.get_clock()
      openingTime = clock.next_open.replace(tzinfo=pytz.UTC).timestamp()
      currTime = clock.timestamp.replace(tzinfo=pytz.UTC).timestamp()
      timeToOpen = int((openingTime - currTime) / 60)
      print(str(timeToOpen) + " minutes til market open.")
      time.sleep(60)
      self.isMarketOpen = self.trade_client.get_clock().is_open

  def nextFriday(self):
      today = datetime.today() 
      next_friday = today + timedelta(5-today.weekday())      
      return next_friday
  
  # getOptionsContracts returns contracts, we want to look at each of them by interrogating contract.symbol. Sample contact SPY240716P00515000
  def getOptionsContracts(self, type, underlying_symbols, expiry_date):
    # specify expiration date range
    now = datetime.now(tz = ZoneInfo("America/New_York"))
    day1 = now + timedelta(days = 0)
    #day60 = now + timedelta(days = 60)

    logging.info("getOptionsContracts: entering")

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
        strike_price_gte = None,                                     # specify strike price range
        strike_price_lte = None,                                     # specify strike price range
        limit = 1000,                                                # specify limit
        page_token = None,                                           # specify page
    )
    res = self.trade_client.get_option_contracts(req)
    Contracts.addContractsFromList(self, new_contracts=res.option_contracts)

    # continue to fetch option contracts if there is next_page_token in response
    if res.next_page_token is not None:
        req = GetOptionContractsRequest(
            underlying_symbols = underlying_symbols,               # specify underlying symbols
            status = AssetStatus.ACTIVE,                           # specify asset status: active (default)
            expiration_date = None,                                # specify expiration date (specified date + 1 day range)
            expiration_date_gte = None,                            # we can pass date object
            expiration_date_lte = None,                            # or string (YYYY-MM-DD)
            root_symbol = None,                                    # specify root symbol
            type = None,                                           # specify option type (ContractType.CALL or ContractType.PUT)
            style = None,                                          # specify option style (ContractStyle.AMERICAN or ContractStyle.EUROPEAN)
            strike_price_gte = None,                               # specify strike price range
            strike_price_lte = None,                               # specify strike price range
            limit = 1000,                                             # specify limit
            page_token = res.next_page_token,                      # specify page token
        )        
        res = self.trade_client.get_option_contracts(req)
        Contracts.addContractsFromList(self, new_contracts=res.option_contracts)

    logging.info("getOptionsContracts: leaving")

  def getNewOptionsContracts(self):

    self.instrument = ["SPY"]

    while True:
      # Get Active Contracts
      underlying_symbols = self.instrument

      # Your background task code here
      nextFriday = self.nextFriday()
      self.getOptionsContracts("call", underlying_symbols, nextFriday)    
      self.getOptionsContracts("put", underlying_symbols, nextFriday)

      time.sleep(30)

  # Retrieve all Contracts for the given underlying stocks ITM
  async def option_data_stream_handler(self, data):
      #publish(data)
      print('options stream update', data)

  def consumer_thread(self):
            
      try:
        self.isConnected = True
        conn.run()

      except Exception as e:
        self.isConnected = False
        print("You got an exception: {} during execution. continue "
            "execution.".format(e))
        # let the execution continue
        pass

  def removeOptionsContracts(self, contracts):
      
      symbols = [
          contracts
      ]

      try:
        if (conn._running):
          conn.unsubscribe_quotes(*symbols)
          conn.unsubscribe_trades(*symbols)
      except Exception as e:
          print("You got an exception: {} during execution. continue "
              "execution.".format(e))
          # let the execution continue
          pass

  def addOptionsContracts(self, contracts):
      
      symbols = [
          contracts
      ]

      try:
        if (conn._running):
          conn.subscribe_quotes(self.option_data_stream_handler, *symbols)
          conn.subscribe_trades(self.option_data_stream_handler, *symbols)        
      except Exception as e:
                print("You got an exception: {} during execution. continue "
                    "execution.".format(e))
                # let the execution continue
                pass


  def stop_consumer_thread(self):     
      if (conn._running):
        conn.stop()

# Run the LongShort class
#ls = Broker()
#print(ls.nextFriday())
#ls.run()
