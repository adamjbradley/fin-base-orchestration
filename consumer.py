#From https://github.com/confluentinc/confluent-kafka-python
import logging

from confluent_kafka import Consumer

c = Consumer({
    'bootstrap.servers': 'localhost',
    'group.id': '3',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['control-topic'])

while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    print('Received message: {}'.format(msg.value().decode('utf-8')))

c.close()