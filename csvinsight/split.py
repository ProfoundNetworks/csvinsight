"""Splits a CSV into multiple columns, one column per file."""
from __future__ import unicode_literals

import collections
import functools
import gzip
import os
import random
import tempfile
import threading

import six
import six.moves.queue as Queue

MAX_QUEUE_SIZE = 10
SENTINEL = None
DEFAULT_BATCH_SIZE = 10000  # Empirically proven to work best
LIST_SEPARATOR = ';'
TEXT_ENCODING = 'utf-8'


def open_temp_file(mode, prefix='tmp'):
    handle, path = tempfile.mkstemp(prefix=prefix, suffix='.gz')
    return gzip.GzipFile(fileobj=os.fdopen(handle, mode), mode=mode), path


class WriterThread(threading.Thread):

    def __init__(self, job_id, thread_id, queue, open_temp=open_temp_file):
        super(WriterThread, self).__init__()
        self._job_id = job_id
        self._id = thread_id
        self._queue = queue
        prefix = 'csvi-%d-%d-' % (self._job_id, self._id)
        self._fout, self._path = open_temp('wb', prefix=prefix)
        self.write = self._write_py2 if six.PY2 else self._write_py3

    def run(self):
        lines = True
        while lines is not SENTINEL:
            lines = self._queue.get()
            if lines is not SENTINEL:
                self.write(lines)
            self._queue.task_done()
        self._fout.close()

    def _write_py2(self, lines):
        self._fout.write((b'\n'.join(lines) + b'\n'))

    def _write_py3(self, lines):
        self._fout.write(('\n'.join(lines) + '\n').encode(TEXT_ENCODING))


def make_batches(iterable, batch_size=DEFAULT_BATCH_SIZE):
    batch = []
    for row in iterable:
        if len(batch) == batch_size:
            yield batch
            batch = []
        batch.append(row)
    if batch:
        yield batch


def _extract_value(value):
    yield value


def _extract_list(value, list_separator=LIST_SEPARATOR):
    for subvalue in value.split(list_separator):
        yield subvalue


def _build_extractors(header, list_columns, list_separator):
    extract_list = functools.partial(_extract_list, list_separator=list_separator)
    return [
        extract_list if column in list_columns else _extract_value
        for column in header
    ]


def populate_queues(header, reader, queues,
                    list_columns=[], list_separator=LIST_SEPARATOR):
    histogram = collections.Counter()
    extractors = _build_extractors(header, list_columns, list_separator)

    for batch in make_batches(reader):
        histogram.update(len(row) for row in batch)
        columns = [list() for _ in header]
        for row in batch:
            if len(header) != len(row):
                continue
            for col_num, value in enumerate(row):
                for subvalue in extractors[col_num](value):
                    columns[col_num].append(subvalue)
        for queue, values in zip(queues, columns):
            queue.put(values)

    for queue in queues:
        queue.put(SENTINEL)
    return histogram


def split(reader, open_file=open_temp_file, list_columns=[], list_separator=LIST_SEPARATOR,
          header=None):
    if six.PY2:
        list_columns = [six.binary_type(col) for col in list_columns]
        list_separator = six.binary_type(list_separator)
    if header is None:
        header = next(reader)
    job_id = random.randint(0, 1000)
    queues = [Queue.Queue(MAX_QUEUE_SIZE) for _ in header]
    threads = [WriterThread(job_id, i, queue, open_temp=open_temp_file)
               for i, queue in enumerate(queues)]
    for thread in threads:
        thread.start()

    histogram = populate_queues(header, reader, queues,
                                list_columns=list_columns, list_separator=list_separator)

    for queue in queues:
        queue.join()

    return header, histogram, [thread._path for thread in threads]


def split_in_memory(reader, list_columns=[], list_separator=LIST_SEPARATOR):
    """Split the CSV reader into columns, in-memory.

    Returns the CSV header.
    Returns a histogram of row lengths (number of columns per row).
    Returns the values of each column as a list.

    Keeps everything in memory, so best used for smaller datasets.

    :arg csv.reader reader: An iterable that yields rows.
    :arg list list_columns: A list of columns that should be split.
    :arg str list_separator: The separator to use when splitting columns.
    :returns: header, histogram, values for each columns
    :rtype: tuple of (list, collections.Counter, list of lists)"""
    if six.PY2:
        list_columns = [six.binary_type(col) for col in list_columns]
        list_separator = six.binary_type(list_separator)
    header = next(reader)
    histogram = collections.Counter()
    columns = [[] for _ in header]
    for i, row in enumerate(reader, 1):
        histogram[len(row)] += 1
        if len(row) != len(header):
            continue
        for j, val in enumerate(row):
            if header[j] in list_columns:
                columns[j].extend(val.split(list_separator))
            else:
                columns[j].append(val)
    return header, histogram, columns
