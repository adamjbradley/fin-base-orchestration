"""
In this example code we will show how to shut the streamconn websocket
connection down and then up again. it's the ability to stop/start the
connection
"""
import logging
import threading
import asyncio
import time
from concurrent.futures import ThreadPoolExecutor

import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from brokeralpaca import Broker
from kafkaproducer import KafkaProducer
from tradingstrategy import Strategy

#Switch to multiprocessing
import multiprocessing as mp

# Please change the following to your own PAPER api key and secret
# You can get them from https://alpaca.markets/

# Alpaca WebSocket Client
# From https://github.com/alpacahq/alpaca-py/blob/master/examples/options-trading-basic.ipynb

api_key = "PKZA540BTUN9GXU7WFYV"
secret_key = "MyklsxHB1K6aqcAZHeGhnMeOYl8h1wCq0c0fwvk1"

#### We use paper environment for this example ####
paper = True # Please do not modify this. This example is for paper trading only.
####

trade_api_url = None
trade_api_wss = None
data_api_url = None
option_stream_data_wss = None

#
instrument = "SPY"
sample_contract = "SPY240716P00515000"
category = "stocks"                             #stocks/options/crypto
isMarketOpen = False

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s  %(levelname)s %(message)s', level=logging.INFO)

    # Initialise Broker
    broker = Broker(api_key, secret_key, paper)

    # Initialise Producer
    producer = KafkaProducer()
        
    # Get Active Contracts
    underlying_symbols = [instrument]

    # Simple Strategy
    strategy = Strategy()
    #contracts_to_watch = strategy.SimpleStrategy(broker.activecontracts)


    # Monitor for market open
    print("Waiting for market to open...")
    wait_for_open = threading.Thread(target=broker.awaitMarketOpen)
    wait_for_open.daemon = True
    wait_for_open.start()

    # Refresh Options Contracts
    options_contracts_thread = threading.Thread(target=broker.getNewOptionsContracts)
    options_contracts_thread.daemon = True
    options_contracts_thread.start()

    
    pool = ThreadPoolExecutor(1)

    while 1:

        if broker.newOptionsContracts:
            logging.info(f"Found {len(broker.allcontracts)} total contracts for {underlying_symbols}")
            # Handle these
            broker.newOptionsContracts = False

        if broker.updatedOptionsContracts:
            logging.info(f"Found {len(broker.updatedcontracts)} updated contracts for {underlying_symbols}")
            # Handle these
            broker.updatedOptionsContracts = False

        if broker.removedOptionsContracts:
            logging.info(f"Found {len(broker.contractstoberemoved)} contracts had been removed for {underlying_symbols}")
            # Handle these
            broker.removeOptionsContracts(broker.contractstoberemoved)
            broker.removedOptionsContracts = False

        if (broker.isMarketOpen):
            try:
                pool.submit(broker.start_option_data_stream())
                time.sleep(2)
                broker.stop_option_data_stream()
                time.sleep(20)
            except KeyboardInterrupt:
                print("Interrupted execution by user")
                broker.stop_option_data_stream()
                exit(0)
            except Exception as e:
                print("You got an exception: {} during execution. continue "
                    "execution.".format(e))
                # let the execution continue
                pass
        else:
            broker.stop_option_data_stream()
        
        time.sleep(1)