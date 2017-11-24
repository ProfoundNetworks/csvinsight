#
# -*- coding: utf-8 -*-
# (C) Copyright: Profound Networks, LLC 2017
#

"""Console script for csvinsight."""
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import argparse
import csv
import collections
import distutils.spawn
import functools
import gzip
import logging
import multiprocessing
import os
import os.path as P
import subprocess
import sys
import tempfile
import yaml

import plumbum
import six

from . import split
from . import summarize

_LOGGER = logging.getLogger(__name__)
_GZIP_MAGIC = b'\x1f\x8b'


def _print_report(header, histogram, results, fout=sys.stdout):
    _print_header(histogram, fout)
    for number, (name, result) in enumerate(zip(header, results), 1):
        result.update(number=number, name=name)
        _print_column_summary(result, fout)


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


def main(stdout=sys.stdout):
    parser = argparse.ArgumentParser()
    parser.add_argument('path', default=None)
    parser.add_argument('--loglevel', default=logging.INFO)
    parser.add_argument('--tempdir', default=tempfile.gettempdir())
    parser.add_argument('--subprocesses', type=int, default=multiprocessing.cpu_count())
    parser.add_argument('--config', default=None)
    parser.add_argument('--delimiter', default='|')
    parser.add_argument('--list-separator', default=';')
    parser.add_argument('--list-fields', nargs='*', default=[])
    args = parser.parse_args()

    logging.basicConfig(level=args.loglevel)
    tempfile.tempdir = args.tempdir

    if args.config:
        with open(args.config) as fin:
            _override_config(fin, args)

    if _is_tiny(args.path):
        with open(args.path) as fin:
            reader = _open_csv(fin, args.delimiter)
            header, histogram, results = _run_in_memory(reader, args)
    else:
        header = _read_header(args.path, args.delimiter)
        part_paths = _split_large_file(args.path)
        histogram, results = _process_multi(header, part_paths, args)
        for part in part_paths:
            os.unlink(part)

    _print_report(header, histogram, results)


def _read_header(path, delimiter):
    """Read the CSV header from the first line of the specified file.

    :arg str path: The file to read.
    :arg str delimiter: The column delimiter.
    :returns: The header
    :rtype: list
    """
    mode = 'rb' if six.PY2 else 'r'

    def open_path():
        if _is_gzipped(path):
            return gzip.GzipFile(path, mode=mode)
        else:
            return open(path, mode)

    with open_path() as fin:
        reader = _open_csv(fin, delimiter)
        return next(reader)


def _is_tiny(path):
    """Return True if the specified file is small enough to process in memory."""
    size_in_mib = os.stat(path).st_size / 1e6
    return (_is_gzipped(path) and size_in_mib < 10) or size_in_mib < 100


def _is_gzipped(path):
    """Returns True if the specified path is a gzipped file."""
    with open(path, 'rb') as fin:
        return fin.read(len(_GZIP_MAGIC)) == _GZIP_MAGIC


def _split_large_file(path, lines_per_part=100000):
    """Split a large file into smaller files.

    Uses GNU command-line tools (e.g. gzip, gsplit) under the cover to
    keep things fast.

    The split result do not include the CSV header.
    Expects the caller to delete the smaller files when done.

    :arg str path: The full path to the file to split.
    :arg str lines_per_part: The max number of lines to include in each part.
    :returns: The path to each part
    :rtype: list
    """
    cat_command = plumbum.local['cat'][path]
    tail_command = plumbum.local['tail']['-n', '+2']

    gzip_exe = _get_exe('pigz', 'gzip')
    gzip_command = plumbum.local[gzip_exe]['--decompress', '--stdout', path]

    prefix = tempfile.mkdtemp(prefix='csvi-')

    split_exe = _get_exe('gsplit', 'split')
    split_flags = ['--filter', "%s > $FILE.gz" % gzip_exe,
                   '--lines=%s' % lines_per_part, '-', prefix + '/']
    split_command = plumbum.local[split_exe][split_flags]

    if _is_gzipped(path):
        chain = gzip_command | tail_command | split_command
    else:
        chain = cat_command | tail_command | split_command

    _LOGGER.info('chain: %s', chain)
    chain()

    full_paths_to_parts = [P.join(prefix, f) for f in os.listdir(prefix)]
    _LOGGER.info('full_paths_to_parts: %r', full_paths_to_parts)
    return full_paths_to_parts


def _get_exe(*preference):
    """Return the first available executable in preference."""
    for exe in preference:
        path = distutils.spawn.find_executable(exe)
        if path:
            return path


def _split_file(header, path, delimiter=None, list_columns=None, list_separator=None):
    """Split a CSV file into columns, one column per file.

    :arg str header: The names for each column of the file.
    :arg str path: The full path to the file.
    :arg str delimiter:
    :arg list list_columns:
    :arg str list_separator:

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

    mode = 'rb' if six.PY2 else 'r'
    with gzip.GzipFile(path, mode=mode) as fin:
        reader = _open_csv(fin, delimiter)
        return split.split(header, reader, list_columns=list_columns,
                           list_separator=list_separator)


def _process_multi(header, paths, args):
    """Process multiple files as multiple subprocesses.

    The multiple processes come in handy for:

        1. Splitting the files into columns
        2. Sorting each column

    Assumes the files contain the same columns.
    Assumes the files are gzipped.

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
        _split_file, header, delimiter=args.delimiter,
        list_columns=args.list_fields, list_separator=args.list_separator
    )
    pool = multiprocessing.Pool(processes=args.subprocesses)

    #
    # each result consists of header, histogram and paths
    #
    results = pool.map(my_split, paths)
    histograms, paths = zip(*results)

    agg_histogram = _aggregate_histograms(histograms)
    agg_paths = _concatenate_tables(paths)

    #
    # We're already running sort_and_summarize in multiple subprocesses, so
    # disable parallelization in that function (num_subprocesses=1).
    #
    my_sort = functools.partial(
        summarize.sort_and_summarize, path_is_gzipped=True,
        compress_temporary=True, num_subprocesses=1
    )
    results = pool.map(my_sort, agg_paths)

    for path in agg_paths:
        os.unlink(path)

    return agg_histogram, results


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


def _concatenate_tables(tables, concatenate=_concatenate):
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


def _open_csv(stream, delimiter):
    if six.PY2:
        delimiter = six.binary_type(delimiter)
    return csv.reader(stream, delimiter=delimiter, quoting=csv.QUOTE_NONE, escapechar=None)


if __name__ == "__main__":
    main()
