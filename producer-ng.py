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
from kafkaconsumerthreaded import KafkaConsumer
from tradingstrategy import Strategy

from config import ALPACA_CONFIG

import random

# Please change the following to your own PAPER api key and secret
# You can get them from https://alpaca.markets/

# Alpaca WebSocket Client
# From https://github.com/alpacahq/alpaca-py/blob/master/examples/options-trading-basic.ipynb

api_key = ALPACA_CONFIG.API_KEY
secret_key = ALPACA_CONFIG.API_SECRET

#api_key = "PKZA540BTUN9GXU7WFYV"
#secret_key = "MyklsxHB1K6aqcAZHeGhnMeOYl8h1wCq0c0fwvk1"

#### We use paper environment for this example ####
paper = True # Please do not modify this. This example is for paper trading only.
####

trade_api_url = None
trade_api_wss = None
data_api_url = None
option_stream_data_wss = None

#
instrument = "SPY"
category = "stocks"                             #stocks/options/crypto

if __name__ == '__main__':
    log = logging.basicConfig(format='%(asctime)s  %(levelname)s %(message)s', level=logging.INFO)

    # Initialise Control Channel
    control_channel = KafkaConsumer("control-topic", "2", "producer-ng")
    control_channel_thread = threading.Thread(target=control_channel.start)
    control_channel_thread.daemon = True
    control_channel_thread.start()

    # Initialise Producer
    producer = KafkaProducer("quickstart-events")
    producer.start("Data Feed: Simple Options Strategy started")

    # Initialise Broker
    broker = Broker(api_key, secret_key, paper, producer, control_channel, log)
        
    # Get Active Contracts
    underlying_symbols = [instrument]

    # Simple Strategy
    #strategy = Strategy()
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

    # WSS Options
    options_data_thread = threading.Thread(target=broker.start_option_data_stream)
    options_data_thread.daemon = True    
    options_data_thread.start()

    # WSS Stocks
    stock_data_thread = threading.Thread(target=broker.start_stock_data_stream)
    stock_data_thread.daemon = True
    stock_data_thread.start()


    while True:

        if broker.newOptionsContracts:
            logging.info(f"Found {len(broker.allcontracts)} total contracts for {underlying_symbols}")
            # Handle these
            broker.addOptionsContracts(broker.contractstobeadded)
            broker.newOptionsContracts = False

        if broker.updatedOptionsContracts:
            logging.info(f"Found {len(broker.updatedcontracts)} updated contracts for {underlying_symbols}")
            # Handle these
            updatedOptionsContracts = broker.updatedOptionsContracts
            broker.updatedOptionsContracts = False

        if broker.removedOptionsContracts:
            logging.info(f"Found {len(broker.contractstoberemoved)} contracts had been removed for {underlying_symbols}")
            # Handle these
            broker.removeOptionsContracts(broker.contractstoberemoved)
            broker.removedOptionsContracts = False

        # Process control messages              
        while len(control_channel.messages) > 0:
            message = control_channel.messages[0]
            logging.info("Processing: " + message)                

            try:
                jsonmessage = json.loads(message)
                logging.info("Messages remaining: " + str(len(control_channel.messages)))
            
                if jsonmessage["action"] == "subscribe.underlying":
                    broker.underlyingsymbols[jsonmessage["parameters"]] = jsonmessage

                if jsonmessage["action"] == "unsubscribe.underlying":
                    del broker.underlyingsymbols[jsonmessage["parameters"]]

            except Exception as e:
                pass

            del control_channel.messages[0]



        if (broker.isMarketOpen):
            try:                
                pass
            except KeyboardInterrupt:
                print("Interrupted execution by user")
                broker.stop_option_data_stream()
                exit(0)
            except Exception as e:
                print("You got an exception: {} during execution. continue "
                    "execution.".format(e))
                # let the execution continue
                pass
                
        time.sleep(1)
        