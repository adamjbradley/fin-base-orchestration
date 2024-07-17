import datetime
import logging

from confluent_kafka import Producer

# Kafka Publish Python Client
# From https://github.com/confluentinc/confluent-kafka-python

class KafkaProducer:
    def __init__(self, _topic):

        logging.basicConfig(format='%(asctime)s  %(levelname)s %(message)s',
            level=logging.INFO)
        
        global producer
        producer = Producer({'bootstrap.servers': 'localhost'})      

        global topic
        topic = _topic
        
    def start(self, origin):        
        producer.produce(topic, origin.encode('utf-8'), callback=self.delivery_report)

    def delivery_report(self, err, msg):
        if err is not None:
            print('Message delivery failed: {}'.format(err))
            self.publish(err)
        else:
            print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

    def publish(self, data):
        producer.poll(0)

        # Asynchronously produce a message. The delivery report callback will
        # be triggered from the call to poll() above, or flush() below, when the
        # message has been successfully delivered or failed permanently.
        producer.produce(topic, data.encode('utf-8'), callback=self.delivery_report)

        # Wait for any outstanding messages to be delivered and delivery report
        # callbacks to be triggered.
        producer.flush()
