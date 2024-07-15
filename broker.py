from confluent_kafka import Producer

import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

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

import broker

#
# Contract/Trade Classes
#

class Contract:
    def __init__(self, name, time, open, last_price):        
        self.last_update_time
        self.name = name
        self.open = open
        self.current = last_price

class Contracts:
    def __init__(self):
        self.contracts = []

class Trade:
    def __init__(self, name, time):        
        self.last_update_time
        self.name = name
        self.buy_price = 0.0
        self.sl = 0.0

class Trades:
    def __init__(self):
        self.trades = []


# Please change the following to your own PAPER api key and secret
# You can get them from https://alpaca.markets/

api_key = "PKZA540BTUN9GXU7WFYV"
secret_key = "MyklsxHB1K6aqcAZHeGhnMeOYl8h1wCq0c0fwvk1"


#### We use paper environment for this example ####
paper = True # Please do not modify this. This example is for paper trading only.
####

# Below are the variables for development this documents
# Please do not change these variables

trade_api_url = None
trade_api_wss = None
data_api_url = None
option_stream_data_wss = None

# check version of alpaca-py
alpaca.__version__

# setup clients
trade_client = TradingClient(api_key=api_key, secret_key=secret_key, paper=paper, url_override=trade_api_url)

# check trading account
# There are trhee new columns in the account object:
# - options_buying_power
# - options_approved_level
# - options_trading_level
acct = trade_client.get_account()

# check account configuration
# - we have new field `max_options_trading_level`
acct_config = trade_client.get_account_configurations()


contracts = Contracts()

def getOptionsContracts(type, underlying_symbols):
    # specify expiration date range
    now = datetime.now(tz = ZoneInfo("America/New_York"))
    day1 = now + timedelta(days = 1)
    day60 = now + timedelta(days = 60)

    req = GetOptionContractsRequest(
        underlying_symbols = underlying_symbols,                     # specify underlying symbols
        status = AssetStatus.ACTIVE,                                 # specify asset status: active (default)
        expiration_date = None,                                      # specify expiration date (specified date + 1 day range)
        expiration_date_gte = day1.date(),                           # we can pass date object
        expiration_date_lte = day60.strftime(format = "%Y-%m-%d"),   # or string
        root_symbol = None,                                          # specify root symbol
        type = type,                                                 # specify option type: put
        style = ExerciseStyle.AMERICAN,                              # specify option style: american
        strike_price_gte = None,                                     # specify strike price range
        strike_price_lte = None,                                     # specify strike price range
        limit = 1000,                                                # specify limit
        page_token = None,                                           # specify page
    )
    res = trade_client.get_option_contracts(req)

    contracts.contracts.extend(res.option_contracts)

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
            limit = 2,                                             # specify limit
            page_token = res.next_page_token,                      # specify page token
        )
        res = trade_client.get_option_contracts(req)
        contracts.contracts.extend(res.option_contracts)

    return res

def subscribe():
    option_data_stream_client = OptionDataStream(api_key, secret_key, url_override = option_stream_data_wss)

    async def option_data_stream_handler(data):
        publish(data)

    symbols = [
       high_open_interest_contract.symbol,
    ]

    option_data_stream_client.subscribe_quotes(option_data_stream_handler, *symbols)
    option_data_stream_client.subscribe_trades(option_data_stream_handler, *symbols)

    option_data_stream_client.run()


def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


def publish(data):
    #for data in some_data_source:
        # Trigger any available delivery report callbacks from previous produce() calls
    p.poll(0)

    # Asynchronously produce a message. The delivery report callback will
    # be triggered from the call to poll() above, or flush() below, when the
    # message has been successfully delivered or failed permanently.
    p.produce('quickstart-events', data.encode('utf-8'), callback=delivery_report)

    # Wait for any outstanding messages to be delivered and delivery report
    # callbacks to be triggered.
    p.flush()




underlying_symbols = ["TSLA","MSFT","SPY"]
res = getOptionsContracts("put", underlying_symbols)

# get high open_interest contract
open_interest = 0
high_open_interest_contract = None
for contract in res.option_contracts:
    if (contract.open_interest is not None) and (int(contract.open_interest) > open_interest):
        open_interest = int(contract.open_interest)
        high_open_interest_contract = contract




p = Producer({'bootstrap.servers': 'localhost'})
subscribe()
