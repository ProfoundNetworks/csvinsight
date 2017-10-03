"""Splits a CSV into multiple columns, one column per file."""
from __future__ import unicode_literals

import collections
import functools
import gzip
import os
import tempfile
import threading
import Queue

MAX_QUEUE_SIZE = 10
SENTINEL = None
DEFAULT_BATCH_SIZE = 10000  # Empirically proven to work best
LIST_SEPARATOR = b';'


def open_temporary_file(mode):
    handle, path = tempfile.mkstemp(suffix='.gz')
    return gzip.GzipFile(fileobj=os.fdopen(handle, mode), mode=mode), path


def writer_thread(queue_in, fout):
    lines = True
    while lines is not SENTINEL:
        lines = queue_in.get()
        if lines is not SENTINEL:
            fout.write(b'\n'.join(lines) + b'\n')
        queue_in.task_done()


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


def split(reader, open_file=open_temporary_file, list_columns=[],
          list_separator=LIST_SEPARATOR):
    header = next(reader)
    fouts, paths = zip(*[open_file('wb') for _ in header])
    queues = [Queue.Queue(MAX_QUEUE_SIZE) for _ in header]
    threads = [threading.Thread(target=writer_thread, args=(queue, fout))
               for (queue, fout) in zip(queues, fouts)]
    for thread in threads:
        thread.start()

    histogram = populate_queues(header, reader, queues,
                                list_columns=list_columns, list_separator=list_separator)

    for queue in queues:
        queue.join()
    for fout in fouts:
        fout.close()

    return header, histogram, paths
