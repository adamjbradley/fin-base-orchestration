import logging
import asyncio
import time

from multiprocessing import Process
from confluent_kafka import Consumer

from ast import literal_eval

from datetime import datetime, timedelta

# Kafka Consumer Python Client
# From https://github.com/confluentinc/confluent-kafka-python
class KafkaConsumer:
    def __init__(self, _topic, _groupid, _consumername):

        logging.basicConfig(format='%(asctime)s  %(levelname)s %(message)s',
            level=logging.INFO)
        
        global consumer
        consumer = Consumer({
            'bootstrap.servers': 'localhost',
            'group.id': _groupid,
            'auto.offset.reset': 'earliest'
        })

        global topic
        topic = _topic
        consumer.subscribe([topic])
        self._consumername = _consumername

        self.messages = []
        
        
    def start(self):
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                time.sleep(5)
                continue
            
            if msg.error():
                logging.info(f"Consumer {self._consumername} error: {msg.error()}")
                continue
            
            message = msg.value().decode('utf-8')
            logging.info(f"Message received {message}")
            self.messages.append(message)

