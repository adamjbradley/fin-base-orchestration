import logging
import threading
import time

from kafkaproducer import KafkaProducer
from kafkaproducer import KafkaProducer
from kafkaconsumerthreaded import KafkaConsumer
from brokeralpaca import Broker

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
#sample_contract = "SPY240716P00515000"
category = "stocks"                             #stocks/options/crypto
isMarketOpen = False


if __name__ == '__main__':
    log = logging.basicConfig(format='%(asctime)s  %(levelname)s %(message)s', level=logging.INFO)


    # Initialise Producer
    producer = KafkaProducer("quickstart-events")
    producer.start("Data Feed: Simple Options Strategy started")

    # Initialise Broker
    broker = Broker(api_key, secret_key, paper, producer, log)

    # Monitor for market open
    print("Waiting for market to open...")
    wait_for_open = threading.Thread(target=broker.awaitMarketOpen)
    wait_for_open.daemon = True
    wait_for_open.start()

    
    # Initialise Consumer
    consumer = KafkaConsumer("quickstart-events", "1")
   
    control_channel = threading.Thread(target=consumer.start)
    control_channel.daemon = True
    control_channel.start()
    

    # Initialise Producer
    producer = KafkaProducer("quickstart-events")
    producer.start("Controller started")


    while True:
        time.sleep(15)
        producer.publish("hope you're still listening!")


    