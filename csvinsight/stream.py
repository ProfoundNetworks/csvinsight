"""Stream implementation of CsvInsight.

Lacks certain attributes like num_uniques and most_common, but is fast."""
from __future__ import division

import collections
import sys

import six

from . import split


class Column(object):
    """Keeps stats for a single column of CSV."""
    def __init__(self):
        self._max_len = 0
        self._min_len = sys.maxsize
        self._sum_len = 0
        self._num_values = 0
        self._num_empty = 0

    @property
    def avg_len(self):
        """The average length for values in this column."""
        return self._sum_len / self._num_values

    def add(self, value):
        """Add a new value to this column.

        :arg str value: A value.  May be an empty string.  May not be None."""
        if not value:
            self._num_empty += 1
        self._max_len = max(self._max_len, len(value))
        self._min_len = min(self._min_len, len(value))
        self._sum_len += len(value)
        self._num_values += 1

    def extend(self, values):
        """Add multiple values to this column.

        :arg list values: A list of string values."""
        for value in values:
            self.add(value)

    def summarize(self):
        """Summarize this column in a single dictionary.

        :returns: A summary of this column.
        :rtype: dict"""
        return {
            'num_values': self._num_values,
            'num_fills': self._num_values - self._num_empty,
            'fill_rate': 100. * (self._num_values - self._num_empty) / self._num_values,
            'max_len': self._max_len,
            'min_len': self._min_len,
            'avg_len': self._sum_len / self._num_values,
            'num_uniques': -1,
            'most_common': [],
        }


def read(reader, list_columns=[], list_separator=split.LIST_SEPARATOR):
    """Split the CSV reader into columns, in-memory.

    Returns the CSV header.
    Returns a histogram of row lengths (number of columns per row).
    Returns a summary of each column as a dict.

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
    columns = [Column() for _ in header]
    for i, row in enumerate(reader, 1):
        histogram[len(row)] += 1
        if len(row) != len(header):
            continue
        for j, val in enumerate(row):
            if header[j] in list_columns:
                columns[j].extend(val.split(list_separator))
            else:
                columns[j].add(val)
    return header, histogram, [col.summarize() for col in columns]
