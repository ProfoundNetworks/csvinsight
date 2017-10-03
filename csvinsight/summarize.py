"""Summarize a single column of values."""
from __future__ import division
import copy
import heapq
import pipes
import sys

MOST_COMMON = 20
BLANK = b''


def run_length_encode(iterator):
    run_value, run_length = next(iterator), 1
    for value in iterator:
        if value < run_value:
            raise ValueError('unsorted iterator')
        elif value != run_value:
            yield run_value, run_length
            run_value, run_length = value, 1
        else:
            run_length += 1
    yield run_value, run_length


class TopN(object):
    def __init__(self, limit=MOST_COMMON):
        self._heap = []
        self._limit = limit

    def push(self, frequency, value):
        if len(self._heap) < self._limit:
            heapq.heappush(self._heap, (frequency, value))
        else:
            lowest_frequency, _ = self._heap[0]
            if frequency > lowest_frequency:
                heapq.heapreplace(self._heap, (frequency, value))

    def to_list(self):
        heapsize = len(self._heap)
        heapcopy = copy.deepcopy(self._heap)
        return [heapq.heappop(heapcopy) for _ in range(heapsize)]


def summarize_sorted(iterator):
    num_values = 0
    num_uniques = 0
    num_empty = 0
    max_len = 0
    min_len = sys.maxint
    sum_len = 0
    topn = TopN()

    for run_value, run_length in run_length_encode(iterator):
        if run_value == BLANK:
            num_empty = run_length
        num_values += run_length
        num_uniques += 1
        val_len = len(run_value)
        max_len = max(max_len, val_len)
        min_len = min(min_len, val_len)
        sum_len += val_len * run_length
        topn.push(run_length, run_value)

    return {
        'num_values': num_values,
        'num_fills': num_values - num_empty,
        'fill_rate': 100. * (num_values - num_empty) / num_values,
        'max_len': max_len,
        'min_len': min_len,
        'avg_len': sum_len / num_values,
        'num_uniques': num_uniques,
        'most_common': list(reversed(topn.to_list())),
    }


def sort_and_summarize(path, gunzip=True):
    template = pipes.Template()
    if gunzip:
        template.append('gunzip -c', '--')
    template.append('LC_ALL=C sort', '--')
    with template.open(path, 'r') as fin:
        result = summarize_sorted(line.rstrip(b'\n') for line in fin)
    return result
