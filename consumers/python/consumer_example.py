from kafka import KafkaConsumer
from time import sleep
from random import randrange, uniform
import json

####################
# Set up variables #
####################
consumer = None
counter = 0

##################
# Set up methods #
##################
def create_consumer():
    consumer = KafkaConsumer('alienvaultdata', bootstrap_servers='broker:9092')
    return consumer

####################
# Sketch variables #
####################
### Common
num_data = 0            # number of data points seen so far

### Moving average of the accuracy radius
MA_acc_rad = 0          # moving average 
MA_n = 5                # n used in MA 
MA_x_array = [0] * MA_n # n long array

### Exponenential average of the accuracy radius
EMA_alpha = 0.6          # weight of the last point
EMA_last_acc_rad = 0     # moving average of the previous timestamp
EMA_acc_rad = 0          # current exponential moving average

### Reservoir sampling
RS_k = 5                 # array length
RS_array = []            # reservoir array

### Morris counter
M_count = 0              # set count c to 0

#####################
# Consumer sketches #
#####################
### Common
def consume_write(message_value):
    global num_data
    num_data += 1
    print(num_data, message_value)

### Moving average of the accuracy radius
def consume_MA_accuracy_radius(message_value):
    global MA_acc_rad, MA_n, MA_x_array, num_data, EMA_last_acc_rad

    EMA_last_acc_rad = MA_acc_rad
    xt = message_value['accuracy_radius']
    oldest_ind = (num_data - MA_n + 1) % MA_n

    MA_acc_rad = MA_acc_rad - (MA_x_array[oldest_ind] / num_data) + (xt / num_data)
    MA_acc_rad = round(MA_acc_rad, 2)

    MA_x_array[oldest_ind] = xt
    print('Moving average of 5 of the accuracy radius:', MA_acc_rad)

### Exponential moving average of the accuracy radius
def consume_EMA_accuracy_radius(message_value):
    global EMA_alpha, EMA_acc_rad, EMA_last_acc_rad

    xt = message_value['accuracy_radius']
    EMA_acc_rad = EMA_alpha * xt + (1-EMA_alpha) * EMA_last_acc_rad
    EMA_acc_rad = round(EMA_acc_rad, 2)

    print('Exponential moving average of 5 of the accuracy radius:', EMA_acc_rad)

### Reservoir sampling
def create_reservoir_object(message_value):
    return {
        'indicator': message_value['indicator'],
        'latitude': message_value['latitude'],
        'longitude': message_value['longitude']
    }

def consume_reservoir_sampling(message_value):
    global RS_k, RS_array

    reservoir_object = create_reservoir_object(message_value)
    t = num_data-1

    if  t < RS_k:
        print('Reservoir sample in initalization phase')
        RS_array.append(reservoir_object)
    
    else:
        r = randrange(RS_k)
        if r < RS_k:
            print('Reservoir sample in update phase replacing index', r)
            RS_array[r] = reservoir_object
        
    print('Reservoir sample', RS_array)

### Moriss' counter
def consume_moriss_counting(message_value):
    global M_count
    
    prob = 1 / (2 ** M_count)
    r = uniform(0, 1)

    if r < prob:
        M_count += 1 

    print('Real Num data', num_data)
    print('Morris’s approximate', 2 ** M_count - 1)

# TODO: add more sketches

############
# Main app #
############
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
        consume_MA_accuracy_radius(message_value)
        consume_EMA_accuracy_radius(message_value)
        consume_reservoir_sampling(message_value)
        consume_moriss_counting(message_value)

        print(' ')

    print('Consumer process successfully ended.')

else:
    print('Consumer can not be initialized. Exiting.')