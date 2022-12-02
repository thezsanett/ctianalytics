from collections import defaultdict


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
            if total <= self._current_bucket_id - self._bucket_id[item]:
                del self._count[item]
                del self._bucket_id[item]

counter = LossyCounting(5e-3)

def consume_heavy_hitters(message_value):

    global counter

    ip = message_value['indicator']
    counter.add_count(ip)
    print(dict(counter._count))