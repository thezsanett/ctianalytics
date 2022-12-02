from kafka import KafkaConsumer
from time import sleep
from random import randrange
from math import sin, cos, sqrt, atan2, radians, inf
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
num_data = 0             # number of data points seen so far

### Moving average of the accuracy radius
MA_acc_rad = 0           # moving average 
MA_n = 5                 # n used in MA 
MA_x_array = [0] * MA_n  # n long array

### Exponenential average of the accuracy radius
EMA_alpha = 0.6          # weight of the last point
EMA_last_acc_rad = 0     # moving average of the previous timestamp
EMA_acc_rad = 0          # current exponential moving average

### Reservoir sampling
RS_k = 5                 # array length
RS_array = []            # reservoir array

### The Leader algorithm
LA_earth_radius = 6373.0 # radius of the earth in km
LA_distance_thr = 250    # distance threshold in km
LA_leaders = []          # leader data point centroids


#####################
# Consumer sketches #
#####################
### Common
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
def consume_reservoir_sampling(message_value):
    global RS_k, RS_array

    reservoir_object = create_sample_object(message_value)
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

### The Leader Algorithm
# Calculating distance using the Haversine formula
def calc_distance(latitude_1, longitude_1, latitude_2, longitude_2):
    global LA_earth_radius
    distance_longitude = longitude_2 - longitude_1
    distance_latitude = latitude_2 - latitude_1

    latitude_squared  = sin(distance_latitude/2) ** 2
    longitude_squared = sin(distance_longitude/2) ** 2

    sub_dist = latitude_squared + cos(latitude_1) * cos(latitude_2) * longitude_squared
    fin_dist = atan2(sqrt(sub_dist), sqrt(1 - sub_dist))

    sphere_distance = 2 * LA_earth_radius * fin_dist
    return sphere_distance

# Finding closest centroid point
def find_closest_centroid(current):
    global LA_leaders

    min_dist = inf
    min_dist_index = -1

    for index, leader in enumerate(LA_leaders):
        dist = calc_distance(radians(leader['latitude']), radians(leader['longitude']), radians(current['latitude']), radians(current['longitude']))
        if dist < min_dist:
            min_dist = dist
            min_dist_index = index

    return min_dist, min_dist_index

# The Leader Algorithm
def consume_leader_algorithm(message_value):
    global LA_leaders, num_data, LA_distance_thr

    leader_object = create_sample_object(message_value)
    closest_dist, index = find_closest_centroid(leader_object)

    if closest_dist < LA_distance_thr:
        LA_leaders[index]['cluster_point_indexes'].append(num_data)
        print('Leader algorithm assigned data point to cluster with index', index)
    else: 
        leader_object['cluster_point_indexes'] = [num_data]
        LA_leaders.append(leader_object)     
        print('Minimal distance from closest cluster centroid is', closest_dist, '-> Leader algorithm created a new leader with index', len(LA_leaders)-1)

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
        consume_leader_algorithm(message_value)

        print('-----')

    print('Consumer process successfully ended.')

else:
    print('Consumer can not be initialized. Exiting.')