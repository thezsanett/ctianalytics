from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from time import sleep
import json

from sketches_sz2jjs import * 
from sketches_w5h8nt import * 

consumer = None
counter = 0

num_data = 0  

cassandra_name = 'cassandra'
cassandra_port = 9042
keyspace_name = 'kafkapipeline'

def create_consumer():
    consumer = KafkaConsumer('alienvaultdata', bootstrap_servers='broker:9092')
    return consumer

def set_up_cassandra():
    global cassandra_name, cassandra_port
    cluster = Cluster([cassandra_name], port=cassandra_port)
    session = cluster.connect(keyspace_name)
    return session

def consume_write(message_value):
    global num_data
    num_data += 1
    print(num_data, message_value)

def create_sample_object(message_value):
    return {
        'indicator': message_value['indicator'],
        'latitude': message_value['latitude'],
        'longitude': message_value['longitude']
    }

############
# Main app #
############

session = set_up_cassandra()
rows = session.execute('SELECT * FROM alienvaultdata;')

for row in rows:
    print(row)

while counter < 10 and consumer is None:
    try:
        print('Trying to connect to Kafka...')
        consumer = create_consumer()
    except:
        print('Kafka is not reachable yet.')
        counter += 1
        sleep(10)

if consumer is not None:
    print('Kafka is up. Printing messages on topic alienvaultdata...')

    for msg in consumer:
        message_value = json.loads(msg.value)

        consume_write(message_value)
        consume_MA_accuracy_radius(message_value, num_data)
        consume_EMA_accuracy_radius(message_value)
        consume_reservoir_sampling(message_value, num_data, create_sample_object)
        consume_leader_algorithm(message_value, num_data, create_sample_object)
        consume_moriss_counting(message_value, num_data)
        consume_space_saving(message_value)

        print('-----')

    print('Consumer process successfully ended.')

else:
    print('Consumer can not be initialized. Exiting.')