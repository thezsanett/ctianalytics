from kafka import KafkaConsumer
from time import sleep
import json

consumer = None
counter = 0

def create_consumer():
    consumer = KafkaConsumer('alienvaultdata', bootstrap_servers='broker:9092')
    return consumer

while counter < 10 and consumer is None:
    try:
        print('Trying to connect to Kafka')
        consumer = create_consumer()
    except:
        print("Kafka is not reachable yet")
        counter += 1
        sleep(15)

if consumer is not None:
    print("Kafka is up. Printing messages on topic alienvaultdata")
    for msg in consumer:
        print(json.loads(msg.value))
    print("Done")
else:
    print("Consumer can not be initialized")