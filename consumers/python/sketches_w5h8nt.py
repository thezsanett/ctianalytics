from random import uniform

####################
# Sketch variables #
####################

### Morris counter
M_count = 0              # set count c to 0

M_insert = "INSERT INTO morriss_counter " \
            "(timestamp, value) " \
            "VALUES (toTimestamp(now()), %s)"

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
            "(id, value, freq, timestamp) " \
            "VALUES (%s, %s, %s, toTimestamp(now()))"

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

    session.execute(M_insert, [M_count])

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
        item_SpaceSaving[count_SpaceSaving.index(my_min)] = radius
        count_SpaceSaving[count_SpaceSaving.index(my_min)] = my_min + 1
        session.execute(SS_update, [radius, my_min + 1, count_SpaceSaving.index(my_min)])
    
    print('SpaceSaving algorithm elements:', item_SpaceSaving)
    print('SpaceSaving algorithm frequencies:', count_SpaceSaving)

