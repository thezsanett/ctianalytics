from kafka import KafkaConsumer
from time import sleep
import json

# Set up variables
consumer = None
counter = 0

# Set up methods
def create_consumer():
    consumer = KafkaConsumer('alienvaultdata', bootstrap_servers='broker:9092')
    return consumer

# Sketch variables
accuracy_radius = 0

# TODO: add more variables

# Consumer sketches
def consume_write(message_value):
    print(message_value)

def consume_add_accuracy_radius(message_value):
    global accuracy_radius
    accuracy_radius += int(message_value['accuracy_radius'])
    print("\nAccuracy radius:", accuracy_radius, '\n')

# TODO: add more sketches

# Main app
while counter < 10 and consumer is None:
    try:
        print('Trying to connect to Kafka...')
        consumer = create_consumer()
    except:
        print("Kafka is not reachable yet.")
        counter += 1
        sleep(15)

if consumer is not None:
    print("Kafka is up. Printing messages on topic alienvaultdata...")

    for msg in consumer:
        message_value = json.loads(msg.value)
        consume_write(message_value)
        consume_add_accuracy_radius(message_value)

    print("Done.")

else:
    print("Consumer can not be initialized. Exiting.")