from random import uniform
import math

####################
# Sketch variables #
####################

### Morris counter
M_count = 0              # set count c to 0

M_insert = "INSERT INTO morriss_counter " \
            "(timestamp, value, uuid) " \
            "VALUES (toTimestamp(now()), %s, 94d6cc06-74b3-11ed-a1eb-0242ac120002)"

### Space Saving
k_SpaceSaving = 3        # 
item_SpaceSaving = []    # 
count_SpaceSaving = []   # 

SS_update = "UPDATE space_saving SET " \
            "value=%s, " \
            "freq=%s, " \
            "timestamp=toTimestamp(now())" \
            "WHERE id=%s"

SS_insert = "INSERT INTO space_saving " \
            "(id, value, freq, timestamp, uuid) " \
            "VALUES (%s, %s, %s, toTimestamp(now()), 94d6cecc-74b3-11ed-a1eb-0242ac120002)"

###DGIM
k = 2 
time = 0
w = 25
count_value = 1000
capacity = []
timestamp = []

DGIM_insert = "INSERT INTO dgim " \
            "(timestamp, value, uuid) " \
            "VALUES (toTimestamp(now()), %s, 94d6d020-74b3-11ed-a1eb-0242ac120002)"


############
# Sketches #
############

### Moriss' counter
def consume_moriss_counting(session, message_value, num_data):
    global M_count
    
    prob = 1 / (2 ** M_count)
    r = uniform(0, 1)

    if r < prob:
        M_count += 1 

    print('Data processed:', num_data, " -> Morris's approximate:", 2 ** M_count - 1)

    session.execute(M_insert, [2 ** M_count - 1])

### Space saving
def consume_space_saving(session, message_value):
    global k_SpaceSaving, item_SpaceSaving, count_SpaceSaving

    radius = message_value['accuracy_radius']

    if radius in item_SpaceSaving:
        count_SpaceSaving[item_SpaceSaving.index(radius)] += 1
        session.execute(SS_update, [radius, count_SpaceSaving[item_SpaceSaving.index(radius)], item_SpaceSaving.index(radius)])

    elif len(item_SpaceSaving) < k_SpaceSaving :
        item_SpaceSaving.append(radius)
        count_SpaceSaving.append(1)
        session.execute(SS_insert, [len(item_SpaceSaving)-1, radius, 1])

    else:
        my_min = min(count_SpaceSaving)
        my_index = count_SpaceSaving.index(my_min)
        item_SpaceSaving[my_index] = radius
        count_SpaceSaving[my_index] = my_min + 1
        session.execute(SS_update, [radius, my_min + 1, my_index])
    
    print('SpaceSaving algorithm elements:', item_SpaceSaving)
    print('SpaceSaving algorithm frequencies:', count_SpaceSaving)


### DGIM
def helper_merge_two_oldest_bucket(size):
    global capacity, timestamp, time
    
    capacity_indeces = [index for (index, item) in enumerate(capacity) if item == size]
    oldest_time = math.inf
    oldest_index = -1
    second_oldest_time = math.inf
    second_oldest_index = -1

    #print('MERGE')
    #helper_print()

    try:
        # merge two oldest buckets at capacity 'size'
        # oldest
        for i in capacity_indeces:
            if timestamp[i] < oldest_time:
                oldest_time = timestamp[i]
                oldest_index = i
        # second
        for i in capacity_indeces:
            if i != oldest_index and timestamp[i] < second_oldest_time:
                second_oldest_time = capacity[i]
                second_oldest_index = i
        #print(f'Merge at index: {oldest_index} and {second_oldest_index}')

        # merge oldest and delete second oldest
        capacity[oldest_index] = size * 2
        timestamp[oldest_index] = time
        timestamp.pop(second_oldest_index)
        capacity.pop(second_oldest_index)
        #print('END MERGE')
        return True
    except:
        print('There is an error during merging in DGIM!')
        return False


def helper_delete_old_buckets(older):
    global capacity, timestamp

    for i, time in enumerate(timestamp):
        if time <= older:
            timestamp.pop(i)
            capacity.pop(i)
    #print('DELETE DGIM SUCCESS')


def helper_print():
    global capacity, timestamp
    print('DGIM results:')

    for i in range(len(capacity)):
        print(f'We have a bucket with capacity {capacity[i]} at time {timestamp[i]}')


def write_to_cassandra_output():
    global capacity, timestamp

    oldest_time = min(timestamp)
    index_oldest = timestamp.index(oldest_time)

    return sum(capacity) + int(capacity[index_oldest]/2)


def consume_dgim(session, message_value):
    global k, w, time, count_value, capacity, timestamp

    time += 1

    if message_value['accuracy_radius'] != count_value:
        return
    
    #Create a bucket at time 't' with capacity '1'
    capacity.append(1)
    timestamp.append(time)
    
    i = 0
    while capacity.count(2**i) > k:
        # merge
        if helper_merge_two_oldest_bucket(2**i):
            i += 1
        else:
            return
    helper_delete_old_buckets(time-w)

    helper_print()

    session.execute(DGIM_insert, [write_to_cassandra_output()])