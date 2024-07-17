import logging
import asyncio
import time

from multiprocessing import Process
from confluent_kafka import Consumer

from ast import literal_eval

class KafkaConsumer:
    def __init__(self, _topic, _groupid):

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
        
    def start(self):
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                time.sleep(5)
                continue
            if msg.error():
                logging.info("Consumer error: {}".format(msg.error()))
                continue

            logging.info('Received message: {}'.format(msg.value().decode('utf-8')))

