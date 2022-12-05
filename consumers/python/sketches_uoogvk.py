import numpy as np

def hash_mod(mod):
    return lambda x: (hash(x) % mod)

# COUNT MIN

class CountMinTable:
    def __init__(self, delta, epsilon):
        self.w = int(2/epsilon)
        self.d = int(np.ceil(np.log(1/delta)))
        self.table = np.zeros((self.d, self.w))
        self.hashes = [hash_mod(self.w) for _ in range(self.d)]
    
    def update(self, value):
        for row,h in enumerate(self.hashes):
            column = h(value)
            self.table[row,column] += 1
    
    def query(self, value):
        counts = []
        for row,h in enumerate(self.hashes):
            column = h(value)
            counts.append(self.table[row,column])
        return int(min(counts))

countmin_insert = "INSERT INTO count_min_output " \
            "(timestamp, ip, count) " \
            "VALUES (toTimestamp(now()), %s, %s)"
countmin_table = CountMinTable(0.0001,0.1)
    
def count_min(session, message_value):
    global countmin_table
    indicator = message_value["indicator"]
    countmin_table.update(indicator)
    count = countmin_table.query(indicator)
    print("Count of [{}]: {}".format(indicator, count))
    session.execute(countmin_insert, [indicator, count])
    
# FLAJOLET MARTIN

class FlajoletMartinBitmap:
    def __init__(self, L):
        self.hash = hash_mod(2**L)
        self.bitmap = np.zeros(L)
        self.L = L
    
    def update(self, value):
        h = self.hash(value)
        index = -1
        for i in range(0, self.L+1):
            bit = h & 2**i
            if bit != 0:
                index = i
                break
        self.bitmap[index] = 1
        
    def query(self):
        R = 0
        for i in range(len(self.bitmap)):
            if self.bitmap[i] == 1:
                R = i
        return (2**R)/0.77351

flajoletmartin_insert = "INSERT INTO flajoletmartin_output " \
            "(timestamp, count) " \
            "VALUES (toTimestamp(now()), %s)"
flajoletmartin_bitmap = FlajoletMartinBitmap(20)
    
def flajolet_martin(session, message_value):
    global flajoletmartin_bitmap
    indicator = message_value["indicator"]
    flajoletmartin_bitmap.update(indicator)
    count = flajoletmartin_bitmap.query()
    print("Distinct elements: {}".format(count))
    session.execute(flajoletmartin_insert, [int(count)])
    
# ITERATIVE K MEANS

class IterativeKMeans:
    def __init__(self, k, number_of_attributes, buffer_size, iterations=100):
        self.buffer = []
        self.buffer_size = buffer_size
        self.iterations = iterations
        self.k = k
        self.number_of_attributes = number_of_attributes
        self.centers = np.random.rand(k,number_of_attributes)
        self.cfs = np.zeros((k,3,number_of_attributes))
        
    def __get_closest_cluster_for(self, element):
        closest = -1
        closest_dist = np.inf
        for i, center in enumerate(self.centers):
            dist = np.linalg.norm(element-center)
            if closest_dist > dist:
                closest = i
                closest_dist = dist
        return closest
        
    def __find_centers(self):
        for _ in range(self.iterations):
            cluster_assignments = np.zeros((len(self.buffer),))
            for i, element in enumerate(self.buffer):
                closest = self.__get_closest_cluster_for(element)
                cluster_assignments[i] = closest
            for i in range(self.k):
                average = np.zeros((self.number_of_attributes,))
                num_of_elements = 0
                for j, assigned in enumerate(cluster_assignments):
                    if assigned == i:
                        average += self.buffer[j]
                        num_of_elements += 1
                average += self.cfs[i][1]
                num_of_elements += self.cfs[i][0][0]
                average /= num_of_elements
                self.centers[i] = average            
        return cluster_assignments

    def __calculate_cfs(self, cluster_assignments):
        for i in range(self.cfs.shape[0]):
            self.cfs[i][0][0] += len(self.buffer)
            for element in self.buffer:
                self.cfs[i][1] += element
                self.cfs[i][2] += np.square(element)
                
    def update(self, value):
        self.buffer.append(value)
        if len(self.buffer) >= self.buffer_size:
            cluster_assignments = self.__find_centers()
            self.__calculate_cfs(cluster_assignments)
            
            self.buffer = []
        
    def query(self, value):
        closest = self.__get_closest_cluster_for(value)
        return closest

kmeans_insert = "INSERT INTO ksparse_output " \
            "(timestamp, point, cluster) " \
            "VALUES (toTimestamp(now()), %s, %s)"
iterativekmeans = IterativeKMeans(k=4,number_of_attributes=2,buffer_size=30)
    
def iterative_kmeans(session, message_value):
    global iterativekmeans
    data = np.asarray((float(message_value["latitude"]), float(message_value["longitude"])))
    iterativekmeans.update(data)
    closest = iterativekmeans.query(data)
    print("Cluster of element ({}): {}".format(data, closest))
    session.execute(kmeans_insert, [str(data), closest])
    
    
