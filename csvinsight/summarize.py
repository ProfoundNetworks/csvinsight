"""Summarize a single column of values."""
from __future__ import division
import copy
import distutils.spawn
import heapq
import multiprocessing
import pipes
import sys
import tempfile

import six

if six.PY2:
    NEWLINE = b'\n'
else:
    NEWLINE = u'\n'

MOST_COMMON = 20


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
    min_len = sys.maxsize
    sum_len = 0
    topn = TopN()

    for run_value, run_length in run_length_encode(iterator):
        if len(run_value) == 0:
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


def _get_exe(*preference):
    for exe in preference:
        path = distutils.spawn.find_executable(exe)
        if path:
            return path


def sort_and_summarize(path, path_is_gzipped=True, compress_temporary=True, buffer_size='2G',
                       num_subprocesses=None):
    if num_subprocesses is None:
        num_subprocesses = multiprocessing.cpu_count()
    #
    # pigz is faster than gzip and therefore better.
    # gsort is always more complete than sort in some environments e.g. macOS
    #
    gzip_exe = _get_exe('pigz', 'gzip')
    sort_exe = _get_exe('gsort', 'sort')
    template = pipes.Template()
    if path_is_gzipped:
        template.append('%s --decompress --stdout' % gzip_exe, '--')
    sort_args = (sort_exe, tempfile.tempdir, num_subprocesses, buffer_size)
    sort_cmd = ('LC_ALL=C %s --temporary-directory=%s '
                '--parallel=%s --buffer-size=%s') % sort_args
    if compress_temporary:
        sort_cmd += ' --compress-program=%s' % gzip_exe
    template.append(sort_cmd, '--')
    with template.open(path, 'r') as fin:
        result = summarize_sorted(line.rstrip(NEWLINE) for line in fin)
    return result
