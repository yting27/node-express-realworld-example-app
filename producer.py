#!/usr/bin/python3

from kafka import KafkaProducer
from kafka.errors import KafkaError
import json


# configure multiple retries
producer = KafkaProducer(bootstrap_servers=['localhost:9093'], # 'localhost:9094', 'localhost:9095' 
                        retries=5, max_in_flight_requests_per_connection=1, 
                        value_serializer=lambda m: json.dumps(m).encode('utf-8'))

# produce json messages
future = producer.send('sample-topic', value={'sales': [2, 4, 7, 19]}, key=b"yes")

# Block for 'synchronous' sends
try:
    record_metadata = future.get(timeout=10)
except KafkaError:
    # Decide what to do if produce request failed...
    log.exception()
    pass

# Successful result returns assigned partition and offset
print(record_metadata.topic)
print(record_metadata.partition)
print(record_metadata.offset)
print("END")


# block until all async messages are sent
# producer.flush()
