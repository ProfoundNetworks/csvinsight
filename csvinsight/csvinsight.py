#
# -*- coding: utf-8 -*-
# (C) Copyright: Profound Networks, LLC 2017
#
from __future__ import division
from __future__ import print_function

import collections
import copy
import functools
import gzip
import json
import heapq
import logging
import multiprocessing
import os
import os.path as P
import shutil
import subprocess

import six

_DELIMITER = b'|'
_LIST_SEPARATOR = b';'
_NUM_WORKERS = 8
_BLANK = b''
_MOST_COMMON = 20
_LOGGER = logging.getLogger(__name__)


class Writer(object):
    def __init__(self, header, open_file):
        self._open_file = open_file
        self._fout = {}

    def write(self, column_name, value):
        try:
            fout = self._fout[column_name]
        except KeyError:
            fout = self._fout[column_name] = self._open_file(column_name)
        fout.write(value + b'\n')

    def close(self):
        for fout in six.itervalues(self._fout):
            fout.close()


class Parser(object):
    """Parses lines into CSV rows/cells."""
    def __init__(self, header, list_fields=[],
                 delimiter=_DELIMITER, list_separator=_LIST_SEPARATOR):
        self._header = header
        self._list_fields = set(list_fields)
        self._counter = collections.Counter()
        self._delimiter = delimiter
        self._list_separator = list_separator

    def parse_line(self, line, writer):
        row = line.rstrip(b'\n').split(self._delimiter)
        self._counter[len(row)] += 1
        if len(row) != len(self._header):
            _LOGGER.error('row length (%d) does not match header length (%d), skipping line %r',
                          len(row), len(self._header), line)
            return

        for col_number, (col_name, cell_value) in enumerate(zip(self._header, row)):
            if col_name in self._list_fields:
                values = cell_value.split(self._list_separator)
            else:
                values = [cell_value]
            for value in values:
                writer.write(col_name, value)


def _map_worker(line_queue, counter_queue, parser, writer):
    while True:
        line = line_queue.get()
        if line is None:
            break
        parser.parse_line(line, writer)
    writer.close()
    counter_queue.put(parser._counter)


def _create_output_dir():
    curr_dir = P.dirname(__file__)
    dir_path = P.join(curr_dir, 'csv_insight.out.%s' % os.getpid())
    if not P.isdir(dir_path):
        os.makedirs(dir_path)
    return dir_path


def _open_file(column_name=None, mode='wb', output_dir=None, suffix=None):
    assert column_name, 'column_name cannot be empty or None'
    return gzip.open(P.join(output_dir, column_name + '.' + suffix), mode)


def map(fin, list_fields=[]):
    output_dir = _create_output_dir()
    header = fin.readline().rstrip().split(_DELIMITER)
    _LOGGER.info('header: %r', header)

    line_queue = multiprocessing.Queue(_NUM_WORKERS * 1000)
    counter_queue = multiprocessing.Queue()

    workers = [
        multiprocessing.Process(
            target=_map_worker,
            args=(
                line_queue, counter_queue, Parser(header, list_fields),
                Writer(
                    header,
                    functools.partial(
                        _open_file, output_dir=output_dir, mode='wb', suffix=str(i)
                    )
                )
            )
        ) for i in range(_NUM_WORKERS)
    ]

    for w in workers:
        w.start()

    for line in fin:
        line_queue.put(line)
    for _ in workers:
        line_queue.put(None)

    for w in workers:
        w.join()

    counter = collections.Counter()
    for _ in workers:
        counter.update(counter_queue.get())

    return header, counter, output_dir


class TopN(object):
    def __init__(self, limit=_MOST_COMMON):
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


def reduce(fin):
    prev_val = fin.readline().rstrip(b'\n')
    num_unique = num_values = run_length = 1
    num_fills = 0 if prev_val == _BLANK else 1
    max_len = min_len = sum_len = len(prev_val)
    topn = TopN(limit=_MOST_COMMON)

    for line in fin:
        line = line.rstrip(b'\n')
        assert line >= prev_val, 'input not sorted (%r < %r), make sure LC_ALL=C' % (line, prev_val)
        if line != prev_val:
            topn.push(run_length, prev_val)
            num_unique += 1
            run_length = 1
        else:
            run_length += 1
        num_values += 1
        if line != _BLANK:
            num_fills += 1
        line_len = len(line)
        max_len = max(line_len, max_len)
        min_len = min(line_len, min_len)
        sum_len += line_len
        prev_val = line
    topn.push(run_length, prev_val)

    return {
        'num_uniques': num_unique,
        'num_values': num_values,
        'num_fills': num_fills,
        'fill_rate': 100 * num_fills / num_values,
        'max_len': max_len,
        'min_len': min_len,
        'avg_len': sum_len / num_values,
        'most_common': list(reversed(topn.to_list()))
    }


def reduce_wrapper(tupl):
    output_dir, column_num, column_name = tupl
    #
    # TODO: use pigz and gsort where possible?
    #
    command = 'cat %s.* | gunzip -c | sort | csvi_reduce' % P.join(output_dir, column_name)
    _LOGGER.info('command: %r', command)
    env = dict(os.environ, LC_ALL='C')
    summary = json.loads(subprocess.check_output(command, shell=True, env=env))
    summary.update({'number': column_num, 'name': column_name})
    return summary


def _print_summary(summary, fout):
    fmt_str1 = ('%(number)d. %(name)s -> Uniques: %(num_uniques)d ; '
                'Fills: %(num_fills)d ; Fill Rate: %(fill_rate).1f%%')
    fmt_str2 = '    Field Length:  min %(min_len)d, max %(max_len)d, avg %(avg_len).2f'
    fmt_str3 = "        {}  {:5.2f} %  {}"
    print(fmt_str1 % summary, file=fout)
    print(fmt_str2 % summary, file=fout)

    num_samples = remainder = summary['num_values']
    print("        Counts      Percent  Field Value", file=fout)
    for count, value in summary['most_common']:
        if value == '':
            value = 'NULL'
        #
        # The json.dumps in reduce_main can cause some encoding weirdness, so
        # deal with it here.
        #
        try:
            value = value.encode('utf-8', errors='replace')
        except UnicodeError:
            value = repr(value)
        print(
            fmt_str3.format(
                str(count).ljust(10), count * 100.0 / num_samples, value
            ), file=fout
        )
        remainder -= count
    if remainder:
        print(
            fmt_str3.format(
                str(remainder).ljust(10), remainder * 100.0 / num_samples, 'Other'
            ), file=fout
        )
    print('', file=fout)


def generate_report(fin, map_kwargs={}):
    _LOGGER.info('starting map')
    header, counter, output_dir = map(fin, **map_kwargs)
    _LOGGER.info('finished map, starting reduce')
    pool = multiprocessing.Pool(processes=_NUM_WORKERS)
    columns = [(output_dir, num, name) for (num, name) in enumerate(header, 1)]
    summaries = pool.map(reduce_wrapper, columns)
    shutil.rmtree(output_dir)
    return {'header': header, 'counter': counter, 'summaries': summaries}


def print_report(report, fout):
    counter = report['counter']
    summaries = report['summaries']

    print('CSV Insight Report', file=fout)
    print('Total # Rows: %d' % sum(counter.values()), file=fout)
    print('Column counts:', file=fout)
    for num_col, freq in sorted(six.iteritems(counter),
                                key=lambda item: item[1], reverse=True):
        print('        %d  columns ->  %d rows' % (num_col, freq), file=fout)
    print("""
Report Format:
Column Number. Column Header -> Uniques: # ; Fills: # ; Fill Rate:
Field Length: min #, max #, average:
 Top n field values -> Dupe Counts

""", file=fout)

    for summary in summaries:
        _print_summary(summary, fout)
