"""Splits a CSV into multiple columns, one column per file."""
import collections
import gzip
import logging
import os
import os.path as P
import queue
import threading

MAX_QUEUE_SIZE = 10
SENTINEL = None
DEFAULT_BATCH_SIZE = 10000  # Empirically proven to work best
LIST_SEPARATOR = ';'
TEXT_ENCODING = 'utf-8'


def _open_temp(subdir, column_id):
    path = P.join(subdir, '%04d.gz' % column_id)
    fout = gzip.GzipFile(path, mode='wb')
    return fout, path


class WriterThread(threading.Thread):
    """Reads column values from a queue and writes them to a temporary file.

    :arg str subdir: The subdirectory where the output file should exist.
    :arg int column_id: The ordinal number of the column being written.
    :arg queue.Queue queue: The queue to read from
    :arg open_temp: A callback for opening a temporary file.
    """
    def __init__(self, subdir, column_id, queue, open_temp=_open_temp):
        super(WriterThread, self).__init__()
        self._column_id = column_id
        self._queue = queue
        self._fout, self._path = open_temp(subdir, column_id)

    def run(self):
        lines = True
        while lines is not SENTINEL:
            lines = self._queue.get()
            if lines is not SENTINEL:
                self._fout.write(('\n'.join(lines) + '\n').encode(TEXT_ENCODING))
            self._queue.task_done()
        self._fout.close()


def make_batches(iterable, batch_size=DEFAULT_BATCH_SIZE):
    batch = []
    for row in iterable:
        if len(batch) == batch_size:
            yield batch
            batch = []
        batch.append(row)
    if batch:
        yield batch


def _populate_queues(header, reader, queues, list_columns=[],
                     list_separator=LIST_SEPARATOR, batch_size=DEFAULT_BATCH_SIZE):
    """Push columns of a csv.Reader into the queues.

    :arg list header: The CSV header - names of the columns.
    :arg csv.Reader reader: The csv.Reader to read from.
    :arg list queues: A list of queues to write to, one queue per column.
    :arg list list_columns:
    :arg str list_separator:
    :arg int batch_size: The maximum number of rows to process as a single batch.
    :returns: A histogram of row lengths
    :rtype: collections.Counter

    The histogram shows the number times each row length was encountered.
    Ideally, all rows would be the same length as the header, but not all CSV
    is like that.
    """
    if len(header) != len(queues):
        raise ValueError('expected one queue per column')

    histogram = collections.Counter()
    list_column_numbers = [i for (i, name) in enumerate(header) if name in list_columns]
    nonlist_column_numbers = [i for (i, name) in enumerate(header) if name not in list_columns]
    assert len(list_column_numbers) + len(nonlist_column_numbers) == len(header)

    #
    # We put batches on the queue, not the actual values themselves.
    # This reduces the overhead (number of calls to Queue.put and .get).
    #
    for batch in make_batches(reader, batch_size=batch_size):
        histogram.update(len(row) for row in batch)
        columns = [list() for _ in header]
        for row in batch:
            if len(header) != len(row):
                continue
            for col_num in list_column_numbers:
                columns[col_num].extend(row[col_num].split(list_separator))
            for col_num in nonlist_column_numbers:
                columns[col_num].append(row[col_num])
        for q, values in zip(queues, columns):
            q.put(values)

    for q in queues:
        q.put(SENTINEL)

    return histogram


def split(header, reader, list_columns=[], list_separator=LIST_SEPARATOR, path=None):
    """Split a CSV reader into columns, one column per temporary file.

    :arg list header: The column names to assume.
    :arg csv.Reader reader: The reader to split.
    :arg list list_columns: Column names to treat as containing lists
    :arg str list_separator: The separator to use when splitting lists
    :arg str path: The path to the file being split.  May not be None.
    :returns: histogram, values for each columns
    :rtype: tuple of (list, collections.Counter, list of lists)

    This function returns three things:

        1. The header
        2. A histogram of row lengths
        3. A list of paths to temporary files, in the same order as the columns.
    """
    if header is None:
        raise ValueError('header may not be None')
    if path is None:
        raise ValueError('path may not be None')

    parts_dir, part_name = P.split(path)
    assert '/parts' in parts_dir, 'expected %r to contain "/parts"' % parts_dir

    columns_dir = parts_dir.replace('/parts', '/columns')
    assert P.isdir(columns_dir), 'expected %r to exist by now' % columns_dir

    part_columns_dir = P.join(columns_dir, part_name)

    logging.debug('parts_dir: %r', parts_dir)
    logging.debug('columns_dir: %r', columns_dir)
    logging.debug('part_columns_dir: %r', part_columns_dir)

    os.mkdir(part_columns_dir)

    queues = [queue.Queue(MAX_QUEUE_SIZE) for _ in header]
    threads = [WriterThread(part_columns_dir, i, q)
               for i, q in enumerate(queues)]
    for thread in threads:
        thread.start()

    histogram = _populate_queues(header, reader, queues,
                                 list_columns=list_columns, list_separator=list_separator)

    for q in queues:
        q.join()

    return histogram, [thread._path for thread in threads]


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
    try:
        header = next(reader)
    except StopIteration:
        raise ValueError('Reader may not be empty')
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
