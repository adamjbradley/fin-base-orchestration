import logging
import threading
import time

from kafkaproducer import KafkaProducer
from kafkaconsumerthreaded import KafkaConsumer
from brokeralpaca import Broker

from datetime import datetime, timedelta

import random

import json

#api_key = "PKZA540BTUN9GXU7WFYV"
#secret_key = "MyklsxHB1K6aqcAZHeGhnMeOYl8h1wCq0c0fwvk1"

#### We use paper environment for this example ####
paper = True # Please do not modify this. This example is for paper trading only.
####

def listToString(thelist):
    return "[\"" + "\",\"".join(str(x) for x in thelist) + "\"]"

def build_control_command(origin, action_type, parameters, ):
    command = {}
    command["action"] = action_type
    command["parameters"] = parameters
    command["origin"] = origin
    command["timestamp"] = datetime.now().isoformat()
    return json.dumps(command)

if __name__ == '__main__':
    log = logging.basicConfig(format='%(asctime)s  %(levelname)s %(message)s', level=logging.INFO)

    # Initialise Control Channel
    consumer = KafkaConsumer("control-topic", "1", "controlclient")
    control_channel = threading.Thread(target=consumer.start)
    control_channel.daemon = True
    control_channel.start()

    # Initialise Producer
    producer = KafkaProducer("quickstart-events")
    producer.start("Data Feed: Simple Options Strategy started" + datetime.now().isoformat())

    # Initialise Broker
    #broker = Broker(api_key, secret_key, paper, producer, log)

    # Monitor for market open
    #print("Waiting for market to open...")
    #wait_for_open = threading.Thread(target=broker.awaitMarketOpen)
    #wait_for_open.daemon = True
    #wait_for_open.start()

    # Initialise Producer
    controller = KafkaProducer("control-topic")
    controller.start("Controller started")

    all_symbols = ['META', 'AAPL', 'SPY', 'QQQ', 'TSLA','NVDA', 'MSFT', 'AMD', 'NFLX']

    heartbeat_command = build_control_command("controlclient", "heartbeat", None)
    get_underlyings_command = build_control_command("controlclient", "getuderlyings", all_symbols)
    
    #sybmols_as_string = listToString(underlying_symbols)
    logging.info("Sending subscribe underlying symbols request to control topic: " + get_underlyings_command)

    subscribe_underlyings = build_control_command("controlclient","subscribe.underlying", "['SPY']")
    controller.publish(subscribe_underlyings)


    while True:

        logging.info("Sending heartbeat to control topic")
        controller.publish(heartbeat_command)

        time.sleep(30)

        random_underlying_index = random.randint(0, len(all_symbols)-1)
        unsubscribe_underlyings = build_control_command("controlclient","subscribe.underlying", all_symbols[random_underlying_index])
        #unsubscribe_underlyings = "{'action':'unsubscribe.underlying','underlying':[\"" + underlying_symbols[random_underlying_index] + "']}"
        logging.info("Sending unsubscribe underlying request to control topic: " + unsubscribe_underlyings)
        controller.publish(unsubscribe_underlyings)

        time.sleep(30)
        random_underlying_index = random.randint(0, len(all_symbols)-1)
        logging.info("Sending subscribe underlying request to control topic: " + all_symbols[random_underlying_index] )
        #subscribe_underlying = "{'action':'subscribe.underlying','underlying':['" + underlying_symbols[random_underlying_index] + "']}"
        subscribe_underlyings = build_control_command("controlclient","unsubscribe.underlying", all_symbols[random_underlying_index])
        controller.publish(subscribe_underlyings)



        time.sleep(30)
