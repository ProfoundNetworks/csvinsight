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
import sys

import six

_DELIMITER = b'|'
_LIST_SEPARATOR = b';'
_NUM_WORKERS = 8
_BLANK = b''
_MOST_COMMON = 20
_LOGGER = logging.getLogger(__name__)


class ColumnSplitter(object):
    """Splits CSV lines into columns, where each column goes to an individual file."""
    def __init__(self, header, open_file, list_fields=[],
                 delimiter=_DELIMITER, list_separator=_LIST_SEPARATOR):
        self._header = header
        self._open_file = open_file
        self._list_fields = set(list_fields)
        self._counter = collections.Counter()
        self._delimiter = delimiter
        self._list_separator = list_separator
        self._fout = {}

    def _write(self, column_name, value):
        try:
            fout = self._fout[column_name]
        except KeyError:
            fout = self._fout[column_name] = self._open_file(column_name)
        fout.write(value + b'\n')

    def close(self):
        for fout in six.itervalues(self._fout):
            fout.close()

    def split_line(self, line):
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
                self._write(col_name, value)


def _map_worker(line_queue, counter_queue, splitter):
    while True:
        line = line_queue.get()
        if line is None:
            break
        splitter.split_line(line)
    splitter.close()
    counter_queue.put(splitter._counter)


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
                line_queue, counter_queue, ColumnSplitter(
                    header,
                    functools.partial(_open_file, output_dir=output_dir, mode='wb', suffix=str(i)),
                    list_fields=list_fields
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


class Column(object):
    """Summarizes a single column of a table, without assumptions about sortedness."""
    def __init__(self, number=1, name='column_name', values_are_lists=False):
        self._number = number
        self._name = name
        self._num_values = 0
        self._num_fills = 0
        self._max_len = 0
        self._min_len = sys.maxint
        self._sum_len = 0
        if values_are_lists:
            self.add = self._add_list
        else:
            self.add = self._add_value

    def _add_list(self, line):
        for value in line.split(_LIST_SEPARATOR):
            self._add_value(value)

    def _add_value(self, line):
        self._num_values += 1
        if line != _BLANK:
            self._num_fills += 1
        line_len = len(line)
        self._max_len = max(line_len, self._max_len)
        self._min_len = min(line_len, self._min_len)
        self._sum_len += line_len

    def finalize(self):
        pass

    def get_summary(self):
        return {
            'number': self._number,
            'name': self._name,
            'num_values': self._num_values,
            'num_uniques': -1,
            'num_fills': self._num_fills,
            'fill_rate': 100 * self._num_fills / self._num_values,
            'max_len': self._max_len,
            'min_len': self._min_len,
            'avg_len': self._sum_len / self._num_values,
        }


class SortedColumn(Column):
    """Summarizes a single column of a table, assuming that it is sorted.

    Sorted columns allow the number of unique values to be calculated easily."""
    def __init__(self, number=1, name='column_name'):
        super(SortedColumn, self).__init__(number=number, name=name, values_are_lists=False)
        self._num_uniques = self._run_length = 0
        self._topn = TopN(limit=_MOST_COMMON)
        self._prev_val = None
        self.add = self._add_value

    def _add_value(self, line):
        if line < self._prev_val:
            raise ValueError(
                'input not sorted (%r < %r), make sure LC_ALL=C' % (line, self._prev_val)
            )

        if self._prev_val is None:
            self._num_uniques = 1
            self._run_length = 1
        elif line != self._prev_val:
            self._topn.push(self._run_length, self._prev_val)
            self._num_uniques += 1
            self._run_length = 1
        else:
            self._run_length += 1
        self._prev_val = line
        super(SortedColumn, self)._add_value(line)
        _LOGGER.debug('self._topn: %r', self._topn)

    def finalize(self):
        self._topn.push(self._run_length, self._prev_val)
        self._prev_val = None
        self._run_length = 0

    def get_summary(self):
        dict_ = super(SortedColumn, self).get_summary()
        dict_.update(most_common=list(reversed(self._topn.to_list())),
                     num_uniques=self._num_uniques)
        return dict_


def reduce(fin, column_class=SortedColumn):
    column = column_class(1, 'column_name')
    for line in fin:
        column.add(line.rstrip(b'\n'))
    column.finalize()
    return column.get_summary()


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

    if summary['num_uniques'] == -1:
        print('', file=fout)
        return

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


def simple_report(fin, list_fields=[]):
    counter = collections.Counter()
    header = fin.readline().rstrip().split(_DELIMITER)
    columns = tuple(Column(number, name, name in list_fields)
                    for (number, name) in enumerate(header, 1))
    for line in fin:
        row = line.rstrip().split(_DELIMITER)
        if len(row) != len(header):
            _LOGGER.error('row length (%d) does not match header length (%d), skipping line %r',
                          len(row), len(header), line)
        counter[len(row)] += 1
        for col, val in zip(columns, row):
            col.add(val)

    for col in columns:
        col.finalize()

    summaries = tuple(col.get_summary() for col in columns)
    return {'header': header, 'counter': counter, 'summaries': summaries}


def full_report(fin, map_kwargs={}):
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
