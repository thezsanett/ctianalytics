from collections import defaultdict
import ipapi
from pysad.models import xStream
from pysad.transform.preprocessing import InstanceUnitNormScaler

class LossyCounting(object):
    'Implemendation of Lossy Counting'

    def __init__(self, epsilon):
        self._n = 0
        self._count = defaultdict(int)
        self._bucket_id = {}
        self._epsilon = epsilon
        self._current_bucket_id = 1

    def add_count(self, item):
        'Add item for counting'
        self._n += 1
        if item not in self._count:
            self._bucket_id[item] = self._current_bucket_id - 1
        self._count[item] += 1

        if self._n % int(1 / self._epsilon) == 0:
            self._trim()
            self._current_bucket_id += 1

    def _trim(self):
        'trim data which does not fit the criteria'
        for item, total in list(self._count.items()):
            try:
                if total <= self._current_bucket_id - self._bucket_id[item]:
                    del self._count[item]
                    del self._bucket_id[item]
            except:
                pass

    def get_sorted_count(self):
        return sorted(self._count.items(), key=lambda k_v: k_v[1], reverse=True)

    def get_count(self, item):
        try:
            return self._count[item]
        except:
            return 0


ip_counter = LossyCounting(5e-3)
country_counter = LossyCounting(5e-3)

ip_heavy_hitters_insert = "INSERT INTO ip_heavy_hitters " \
                            "(ip, frequency) " \
                            "VALUES (%s, %s)"


def consume_ip_heavy_hitters(session, message_value, to_print=False):

    global ip_counter

    ip = message_value['indicator']
    ip_counter.add_count(ip)
    if to_print:
        print(ip_counter.get_sorted_count())
    session.execute(ip_heavy_hitters_insert, [ip, ip_counter.get_count(ip)])

country_heavy_hitters_insert = "INSERT INTO country_heavy_hitters " \
                                "(country, frequency) " \
                                "VALUES (%s, %s)"

def consume_country_heavy_hitters(session, message_value, to_print=False):

    global country_counter

    country = message_value['country_name']
    try:
        country_counter.add_count(country)
        if to_print:
            print(country_counter.get_sorted_count())
        session.execute(country_heavy_hitters_insert, [country, country_counter.get_count(country)])
    except:
        pass

def consume_data_enrichment(message_value):

    data =  ipapi.location(ip=message_value['indicator'])

    print(data)


model = xStream()  
preprocessor = InstanceUnitNormScaler() 

def consume_outlier_detection(message_value):


    global model
    global preprocessor

    x = preprocessor.fit_transform_partial([message_value['latitude']]) 

    score = model.fit_score_partial(x)  
    print(f"anomalousness score = {score[0]}")
