import logging

import datetime
import json

from confluent_kafka import Consumer

# Kafka Consumer Python Client
# From https://github.com/confluentinc/confluent-kafka-python


class ControlMessage:    
    def __init__(self, _message):
        try:
            result = json.dumps(message)
            control_message = ControlMessage()
            self.operation = result.operation
            self.subject = result.subject
            self.datetimereceived = result.timestamp
            self.responserequired = result.responserequired

        except Exception as e:
            pass

class KafkaConsumer:
    def __init__(self, _topic):

        global topic
        topic = _topic

        logging.basicConfig(format='%(asctime)s  %(levelname)s %(message)s',
            level=logging.INFO)
        
        global consumer
        consumer = Consumer({
            'bootstrap.servers': 'localhost',
            'group.id': 'mygroup',
            'auto.offset.reset': 'earliest'
        })      

        self.messagequeue = {}
        
    def start(self, origin):        
        consumer.subscribe([topic])

        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue

            print('Received message: {}'.format(msg.value().decode('utf-8')))

            message = ControlMessage(msg.value().decode('utf-8'))
            self.messagequeue[message.datetimereceived + "-" + message.subject] = message

        c.close()

if __name__ == '__main__':
    log = logging.basicConfig(format='%(asctime)s  %(levelname)s %(message)s', level=logging.INFO)


