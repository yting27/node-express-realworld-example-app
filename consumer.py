#!/usr/bin/python3

from kafka import KafkaConsumer
import json

# consume json messages
consumer = KafkaConsumer('sample-topic', group_id='group1', client_id="sample_client",
                            bootstrap_servers=['localhost:9093'], # ['localhost:9093', 'localhost:9094', 'localhost:9095'],
                            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                            key_deserializer=lambda k: k.decode("utf-8"))

for message in consumer:
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value))

# # consume earliest available messages, don't commit offsets
# KafkaConsumer(auto_offset_reset='earliest', enable_auto_commit=False)

# # StopIteration if no message after 1sec
# KafkaConsumer(consumer_timeout_ms=1000)

# Use multiple consumers in parallel w/ 0.9 kafka brokers
# typically you would run each on a different server / process / CPU
# consumer1 = KafkaConsumer('my-topic',
#                           group_id='my-group',
#                           bootstrap_servers='my.server.com')
# consumer2 = KafkaConsumer('my-topic',
#                           group_id='my-group',
#                           bootstrap_servers='my.server.com')