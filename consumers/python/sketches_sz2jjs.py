from math import sin, cos, sqrt, atan2, radians, inf
from random import randrange

####################
# Sketch variables #
####################

### Moving average of the accuracy radius
MA_acc_rad = 0           # moving average 
MA_n = 5                 # n used in MA 
MA_x_array = [0] * MA_n  # n long array

MA_insert = "INSERT INTO moving_average " \
            "(timestamp, value) " \
            "VALUES (toTimestamp(now()), %s)"

### Exponenential average of the accuracy radius
EMA_alpha = 0.6          # weight of the last point
EMA_last_acc_rad = 0     # moving average of the previous timestamp
EMA_acc_rad = 0          # current exponential moving average

EMA_insert = "INSERT INTO exponential_moving_average " \
            "(timestamp, value) " \
            "VALUES (toTimestamp(now()), %s)"

### Reservoir sampling
RS_k = 5                 # array length
RS_array = []            # reservoir array

RS_update = "UPDATE reservoir_sample SET " \
            "value=%s, " \
            "timestamp=toTimestamp(now())" \
            "WHERE id=%s"

RS_insert = "INSERT INTO reservoir_sample " \
            "(id, value, timestamp) " \
            "VALUES (%s, %s, toTimestamp(now()))"

### The Leader algorithm
LA_earth_radius = 6373.0 # radius of the earth in km
LA_distance_thr = 250    # distance threshold in km
LA_leaders = []          # leader data point centroids

LA_update = "UPDATE leader_algorithm_cluster " \
            "SET points=%s " \
            "WHERE id=%s"

LA_insert = "INSERT INTO leader_algorithm_cluster " \
            "(id, points, latitude, longitude, timestamp) " \
            "VALUES (%s, 1, %s, %s, toTimestamp(now()))"


############
# Sketches #
############

### Moving average of the accuracy radius
def consume_MA_accuracy_radius(session, message_value, num_data):
    global MA_acc_rad, MA_n, MA_x_array, EMA_last_acc_rad, MA_insert

    EMA_last_acc_rad = MA_acc_rad
    xt = message_value['accuracy_radius']
    oldest_ind = (num_data - MA_n + 1) % MA_n

    MA_acc_rad = MA_acc_rad - (MA_x_array[oldest_ind] / num_data) + (xt / num_data)
    MA_acc_rad = round(MA_acc_rad, 2)

    MA_x_array[oldest_ind] = xt
    print('Moving average of 5 of the accuracy radius:', MA_acc_rad)
    
    session.execute(MA_insert, [MA_acc_rad])

### Exponential moving average of the accuracy radius
def consume_EMA_accuracy_radius(session, message_value):
    global EMA_alpha, EMA_acc_rad, EMA_last_acc_rad, EMA_insert

    xt = message_value['accuracy_radius']
    EMA_acc_rad = EMA_alpha * xt + (1-EMA_alpha) * EMA_last_acc_rad
    EMA_acc_rad = round(EMA_acc_rad, 2)

    print('Exponential moving average of 5 of the accuracy radius:', EMA_acc_rad)

    session.execute(EMA_insert, [EMA_acc_rad])

### Reservoir sampling
def consume_reservoir_sampling(session, message_value, num_data, create_sample_object):
    global RS_k, RS_array, RS_update, RS_insert

    reservoir_object = create_sample_object(message_value)
    t = num_data-1

    if  t < RS_k:
        print('Reservoir sample in initalization phase')
        RS_array.append(reservoir_object)
        session.execute(RS_insert, [len(RS_array)-1, reservoir_object['indicator']])
    
    else:
        r = randrange(t)
        if r < RS_k:
            print('Reservoir sample in update phase replacing index', r)
            RS_array[r] = reservoir_object
            session.execute(RS_update, [reservoir_object['indicator'], r])

        
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
def consume_leader_algorithm(session, message_value, num_data, create_sample_object):
    global LA_leaders, LA_distance_thr, LA_update, LA_insert

    leader_object = create_sample_object(message_value)
    closest_dist, index = find_closest_centroid(leader_object)

    if closest_dist < LA_distance_thr:
        LA_leaders[index]['cluster_point_indexes'].append(num_data)
        print('Leader algorithm assigned data point to cluster with index', index)
        session.execute(LA_update, [len(LA_leaders[index]['cluster_point_indexes']), index])
    else: 
        leader_object['cluster_point_indexes'] = [num_data]
        LA_leaders.append(leader_object)     
        print('Minimal distance from closest cluster centroid is', closest_dist, '-> Leader algorithm created a new leader with index', len(LA_leaders)-1)
        session.execute(LA_insert, [len(LA_leaders)-1, leader_object['latitude'], leader_object['longitude']])