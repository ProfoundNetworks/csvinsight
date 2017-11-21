#
# -*- coding: utf-8 -*-
# (C) Copyright: Profound Networks, LLC 2017
#

"""Console script for csvinsight."""
from __future__ import print_function
from __future__ import unicode_literals

import argparse
import csv
import collections
import functools
import json
import logging
import multiprocessing
import os
import subprocess
import sys
import tempfile
import yaml

import six

from . import split
from . import stream
from . import summarize

_LOGGER = logging.getLogger(__name__)


def _add_default_args(parser):
    parser.add_argument('--loglevel', default=logging.INFO)
    parser.add_argument('--tempdir', default=tempfile.gettempdir())


def _add_map_args(parser):
    parser.add_argument('--delimiter', default='|')
    parser.add_argument('--list-separator', default=';')
    parser.add_argument('--list-fields', nargs='*', default=[])


def _print_header(histogram, fout):
    print('CSV Insight Report', file=fout)
    print('Total # Rows: %d' % sum(histogram.values()), file=fout)
    print('Column counts:', file=fout)
    for num_col, freq in sorted(six.iteritems(histogram),
                                key=lambda item: item[1], reverse=True):
        print('        %d  columns ->  %d rows' % (num_col, freq), file=fout)
    print("""
Report Format:
Column Number. Column Header -> Uniques: # ; Fills: # ; Fill Rate:
Field Length: min #, max #, average:
 Top n field values -> Dupe Counts

""", file=fout)


def _print_column_summary(summary, fout):
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


def _process_full(reader, args):
    header, histogram, paths = split.split(
        reader, list_columns=args.list_fields, list_separator=args.list_separator
    )

    pool = multiprocessing.Pool(processes=args.subprocesses)
    results = pool.map(summarize.sort_and_summarize, paths)

    for path in paths:
        os.unlink(path)

    return header, histogram, results


def _run_in_memory(reader, args):
    header, histogram, columns = split.split_in_memory(
        reader, list_columns=args.list_fields, list_separator=args.list_separator
    )
    column_summaries = [summarize.summarize_sorted(iter(sorted(col))) for col in columns]
    return header, histogram, column_summaries


def _override_config(fin, args):
    config = yaml.load(fin)
    args.delimiter = config.get('delimiter', args.delimiter)
    args.list_separator = config.get('list_separator', args.list_separator)
    args.list_fields = config.get('list_fields', args.list_fields)
    args.header = config.get('header')


def parse_args(args):
    #
    # FIXME: args is not being used here
    #
    parser = argparse.ArgumentParser()
    parser.add_argument('--subprocesses', type=int, default=multiprocessing.cpu_count())
    parser.add_argument('--file', default=None)
    parser.add_argument('--config', default=None)
    parser.add_argument('--stream', action='store_true')
    parser.add_argument('--in-memory', action='store_true')
    _add_default_args(parser)
    _add_map_args(parser)
    return parser.parse_args(args)


def main(argv=sys.argv[1:], stdin=sys.stdin, stdout=sys.stdout):
    args = parse_args(argv)
    _LOGGER.debug('args: %r', args)

    tempfile.tempdir = args.tempdir
    logging.basicConfig(level=args.loglevel)

    if args.file:
        in_stream = open(args.file)
    else:
        in_stream = stdin

    if args.config:
        with open(args.config) as fin:
            _override_config(fin, args)
    _LOGGER.info('args: %r', args)

    reader = _open_csv(in_stream, delimiter=args.delimiter)
    if args.stream:
        header, histogram, results = stream.read(
            reader, list_columns=args.list_fields, list_separator=args.list_separator
        )
    elif args.in_memory:
        header, histogram, results = _run_in_memory(reader, args)
    else:
        header, histogram, results = _process_full(reader, args)

    _print_header(histogram, stdout)
    for number, (name, result) in enumerate(zip(header, results), 1):
        result.update(number=number, name=name)
        _print_column_summary(result, stdout)


def main_multi(stdout=sys.stdout):
    parser = argparse.ArgumentParser()
    parser.add_argument('--subprocesses', type=int, default=multiprocessing.cpu_count())
    parser.add_argument('--files', nargs='+')
    parser.add_argument('--config', default=None)
    _add_default_args(parser)
    _add_map_args(parser)
    args = parser.parse_args()

    logging.basicConfig(level=args.loglevel)

    if args.config:
        with open(args.config) as fin:
            _override_config(fin, args)

    header, histogram, results = _process_multi(args)

    _print_header(histogram, stdout)
    for number, (name, result) in enumerate(zip(header, results), 1):
        result.update(number=number, name=name)
        _print_column_summary(result, stdout)


def _split_file(path, delimiter=None, list_columns=None, list_separator=None,
                header=None):
    """Split a CSV file into columns, one column per file.

    Returns the header, the row length histogram, and the paths of the files
    storing each column.
    """
    #
    # This is here because:
    #
    # 1. This function runs as a separate process, and receives pickled args
    # 2. We cannot pickle a csv.Reader
    # 3. split.split expects a csv.Reader
    #
    assert delimiter
    assert list_separator
    with open(path, 'rb' if six.PY2 else 'r') as fin:
        reader = _open_csv(fin, delimiter)
        return split.split(reader, list_columns=list_columns,
                           list_separator=list_separator, header=header)


def _process_multi(args):
    """Process multiple files as multiple subprocesses.

    The multiple processes come in handy for:

        1. Splitting the files into columns
        2. Sorting each column

    Assumes the files contain the same columns.

    Returns a header, the row length histogram, and a dictionary summary of the
    results.
    """
    #
    # Use multiple processes for splitting the N input files.
    # This gives us N sets of M columns.
    # After splitting, aggregate the split results.
    # This gives us a single set of M columns.
    #
    my_split = functools.partial(
        _split_file, delimiter=args.delimiter, list_columns=args.list_fields,
        list_separator=args.list_separator, header=args.header
    )
    pool = multiprocessing.Pool(processes=args.subprocesses)
    #
    # each result consists of header, histogram and paths
    #
    results = pool.map(my_split, args.files)
    headers, histograms, paths = zip(*results)

    agg_histogram = _aggregate_histograms(histograms)
    agg_paths = _aggregate_paths(paths)

    results = pool.map(summarize.sort_and_summarize, agg_paths)

    for path in agg_paths:
        os.unlink(path)

    return headers[0], agg_histogram, results


def _check_headers(headers):
    for h in headers:
        if h != headers[0]:
            raise ValueError('the files contain different headers')


def _aggregate_histograms(histograms):
    """Aggregate multiple histograms into one.

    A single histogram just contains the number of times each row length
    occurred in the dataset.

    :arg list histograms: The histograms, as collections.Counter objects.
    :returns: A histogram
    :rtype: collections.Counter
    """
    aggregated = collections.Counter()
    for hist in histograms:
        aggregated.update(hist)
    return aggregated


def _aggregate_paths(tables):
    """Concatenate the tables together to form one big table.

    Each table is a list of columns, where a column is stored in a separate
    file.  To create the big table, this function concatenates the respective
    columns together.

    Each table must contain the same number of columns for this to work.

    Deletes the files from the individual tables after concatenating.

    :arg list tables: A list of tables.
    :returns: A table, as a list of concatenated columns.
    :rtype: list
    :raises ValueError: if the tables contain a different number of columns
    """
    num_columns = len(tables[0])
    for tbl in tables:
        if not len(tbl) == num_columns:
            raise ValueError('number of columns must be the same for each table')

    concat_paths = []
    for column_number in range(num_columns):
        concat_paths.append(_concatenate([tbl[column_number] for tbl in tables]))

    return concat_paths


def _concatenate(paths):
    """Concatenate the specified files together into a single file.

    Deletes the files once the concatenation is complete.

    Returns the new file path."""
    handle, path = tempfile.mkstemp()

    with os.fdopen(handle, 'wb') as fout:
        command = ['cat'] + list(paths)
        subprocess.check_call(command, stdout=fout)

    for p in paths:
        os.unlink(p)

    return path


def _open_csv(stream, delimiter):
    if six.PY2:
        delimiter = six.binary_type(delimiter)
    return csv.reader(stream, delimiter=delimiter, quoting=csv.QUOTE_NONE, escapechar=None)


def main_split():
    parser = argparse.ArgumentParser()
    _add_default_args(parser)
    _add_map_args(parser)
    args = parser.parse_args()

    logging.basicConfig(level=args.loglevel)
    tempfile.tempdir = args.tempdir

    reader = _open_csv(sys.stdin, args.delimiter)
    header, histogram, paths = split.split(
        reader, list_columns=args.list_fields, list_separator=args.list_separator
    )
    for column, path in zip(header, paths):
        print(column, path)


def main_summarize():
    parser = argparse.ArgumentParser()
    _add_default_args(parser)
    args = parser.parse_args()
    logging.basicConfig(level=args.loglevel)

    summary = summarize.summarize_sorted(line.rstrip(summarize.NEWLINE) for line in sys.stdin)
    print(json.dumps(summary) + summarize.NEWLINE)


if __name__ == "__main__":
    main()
