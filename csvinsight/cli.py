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
import codecs
import collections
import distutils.spawn
import functools
import gzip
import json
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
_MAX_ARGS = 100
"""The max number of arguments to pass to a single subprocess call."""

_LINES_PER_PART = 100000


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
    column_summaries = [
        summarize.summarize_sorted(iter(sorted(col)), most_common=args.most_common)
        for col in columns
    ]
    return header, histogram, column_summaries


_CSV_DIALECT_PARAMS = {
    'delimiter': b'|' if six.PY2 else '|',
    'quotechar': b'\'' if six.PY2 else '\'',
    'escapechar': b'\\' if six.PY2 else '\\',
    'doublequote': b'"' if six.PY2 else '"',
    'skipinitialspace': 'True',
    'lineterminator': b'\n' if six.PY2 else '\n',
    'quoting': 'QUOTE_NONE'
}


class Dialect(csv.Dialect):
    def __init__(self, **kwargs):
        for param, defaultvalue in _CSV_DIALECT_PARAMS.items():
            setattr(self, param, kwargs.get(param, defaultvalue))

        self.skipinitialspace = self.skipinitialspace.lower() in ('true', 't', '1')

        #
        # There are two ways to specify the quoting parameter: either directly,
        # as an integer (as it's defined in the csv submodule), or as a string
        # literal, e.g. "QUOTE_NONE".
        #
        if self.quoting.isdigit():
            self.quoting = int(self.quoting)
        elif not self.quoting.startswith('QUOTE_'):
            raise ValueError('unsupported quoting method: %r' % self.quoting)
        else:
            try:
                self.quoting = getattr(csv, self.quoting)
            except AttributeError:
                raise ValueError('unsupported quoting method: %r' % self.quoting)

    def __repr__(self):
        params = ['%s=%r' % (key, getattr(self, key)) for key in _CSV_DIALECT_PARAMS]
        return 'Dialect(%s)' % ', '.join(params)


def _parse_dialect(pairs_as_strings):
    _LOGGER.debug('locals: %r', locals())
    if six.PY2:
        kwargs = dict(six.binary_type(pair).split(b'=', 1) for pair in pairs_as_strings)
    else:
        kwargs = dict(six.text_type(pair).split(u'=', 1) for pair in pairs_as_strings)
    dialect = Dialect(**kwargs)
    # dialect._validate()
    return dialect


def _override_config(fin, args):
    config = yaml.load(fin)
    args.list_separator = config.get('list_separator', args.list_separator)
    args.list_fields = config.get('list_fields', args.list_fields)

    #
    # By inserting at the start of the list, we allow command-line arguments
    # to override the configuration file.
    #
    for key in _CSV_DIALECT_PARAMS:
        if key in config:
            args.dialect.insert(0, '%s=%s' % (key, config[key]))


def _generate_ipython_report(ipynb_path, report_as_json):
    from . import ipynb
    with open(ipynb_path, 'w') as fout:
        fout.write(ipynb.generate(report_as_json))
    ipynb.execute(ipynb_path, save_html=True)


def main(stdout=sys.stdout):
    csv.field_size_limit(sys.maxsize)

    log_levels = ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')
    description = 'Read a CSV file and determine unique values for each column'
    epilog = """\
If possible, install pigz on your system.  It makes better use of multiple
cores when compressing and decompressing.

Writes temporary files to disk, so make sure --tempdir is set to something with
plenty of space.

CSV dialects are specified as space-separated key-value pairs, for example:

    csvi file.csv --dialect delimiter=, quoting=QUOTE_ALL

For the list of available dialect parameters, see:

    https://docs.python.org/2/library/csv.html#dialects-and-formatting-parameters
    """
    parser = argparse.ArgumentParser(
        description=description, formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=epilog
    )
    parser.add_argument('path', default=None, help='The path to the CSV file to open')
    parser.add_argument('--loglevel', choices=log_levels, default=logging.INFO)
    parser.add_argument('--tempdir', default=tempfile.gettempdir(), metavar='SUBDIR',
                        help='The directory to which temporary files will be written')
    parser.add_argument('--subprocesses', type=int, default=multiprocessing.cpu_count(),
                        metavar='NUM_CORES', help='The number of subprocesses to use')
    parser.add_argument('--config', default=None, metavar='PATH',
                        help='The path to the configuration file.')
    parser.add_argument('--dialect', nargs='+', default=[], metavar='KEY=VALUE',
                        help='The CSV dialect to use when parsing the file')
    parser.add_argument('--list-fields', nargs='*', default=[], metavar='FIELD_NAME',
                        help='The names of fields that contain lists instead of atomic values')
    parser.add_argument('--list-separator', default=';', metavar='CHARACTER',
                        help='The separator used to split lists into atomic values')
    parser.add_argument(
        '--most-common', default=summarize.MOST_COMMON, type=int,
        help='The number of most common values to show for each column'
    )
    parser.add_argument(
        '--no-tiny', action='store_true',
        help='Skip the in-memory optimization for tiny CSV files'
    )
    parser.add_argument(
        '--lines-per-part', default=_LINES_PER_PART,
        help='The number of lines in each part when splitting large files'
    )
    parser.add_argument(
        '--json',
        help='Write a JSON version of the report to this file'
    )
    parser.add_argument(
        '--ipynb',
        help='Write a Python notebook version of the report to this file. '
             'Requires an optional Jupyter dependency.'
    )
    args = parser.parse_args()

    logging.basicConfig(level=args.loglevel)
    tempfile.tempdir = args.tempdir

    if args.config:
        with open(args.config) as fin:
            _override_config(fin, args)

    if args.ipynb:
        try:
            from . import ipynb  # noqa
        except ImportError:
            _LOGGER.critical(
                "Could not import jupyter library. Please install it via pip by calling "
                "pip install jupyter or pip install csvinsight[notebook]"
            )
            sys.exit(1)

    csv_dialect = _parse_dialect(args.dialect)

    #
    # To process a large file, we do the following:
    #
    # 1. Split the larger file into multiple equal-sized smaller parts.
    # 2. Split each smaller part into columns.
    # 3. Process each column individually
    #
    # We do (1) because it enables us to do (2) using multiple processes.
    #
    # The above process means we need to use M * N temporary files, where:
    #
    # - M = number of parts (num_lines_in_file / part_size)
    # - N = number of columns
    #
    # We organize the files as follows.
    #
    # (1) creates /TMPDIR/csvi-xxxx/parts/yy.gz where TMPDIR comes from
    # the tempfile library, xxxx is a random number and yy is the number of
    # the part.
    #
    # (2) creates /TMPDIR/csvi-XXXX/columns/yy/zz.gz, where zz is the column
    # number, and all the other variables are the same as above.
    #
    # The above allows us to split the CSV file into columns quickly and
    # without loading large parts of it into memory at once.  Of course,
    # the above is a complete waste of time for files that easily fit into
    # memory, so we shortcut the process in that case.
    #
    if _is_tiny(args.path) and not args.no_tiny:
        with _open_for_reading(args.path) as fin:
            reader = csv.reader(fin, dialect=csv_dialect)
            header, histogram, results = _run_in_memory(reader, args)
    else:
        with _open_for_reading(args.path) as fin:
            header = next(csv.reader(fin, dialect=csv_dialect))
        part_paths = _split_large_file(args.path, lines_per_part=args.lines_per_part)
        histogram, results = _process_multi(header, part_paths, csv_dialect, args)
        for part in part_paths:
            os.unlink(part)

    report_as_json = {
        'path': args.path,
        'histogram': histogram,
        'header': header,
        'results': {
            name: results
            for (name, results) in zip(header, results)
        },
    }

    if args.json:
        with open(args.json, 'w') as fout:
            json.dump(report_as_json, fout)

    if args.ipynb:
        _generate_ipython_report(args.ipynb, report_as_json)
    #
    # Reconcile differences between Py2 and Py3 here.
    # _print_report expects strings and writes strings.
    # Under Py2, everything until now outputs bytes, so we need to decode.
    #
    if six.PY2:
        header = [h.decode('utf-8') for h in header]
        for summary in results:
            summary['most_common'] = [
                (count, value.decode('utf-8'))
                for (count, value) in summary['most_common']
            ]
        fout = codecs.getwriter('utf-8')(sys.stdout)
    else:
        fout = sys.stdout

    _print_report(header, histogram, results, fout=fout)


def _open_for_reading(path, encoding='utf-8'):
    """Opens a file for reading.

    Under Python 2, this means reading in binary mode, because that's what the
    Py2 CSV module expects.  Under Py3, this means reading in text mode.

    Transparently handles gzipped files.

    :param str path: The path to open.
    :param str encoding: The encoding to use when reading in text mode.
    :returns: A file object
    :rtype: fileobj
    """
    if _is_gzipped(path):
        fin = gzip.GzipFile(path, mode='rb')
        if six.PY3:
            #
            # GzipFile docs state that it supports outputting text, but this
            # doesn't seem so in practice, so we take care of it ourselves.
            #
            fin = codecs.getreader(encoding)(fin)
        return fin
    else:
        return open(path, 'r' if six.PY3 else 'rb')


def _is_tiny(path):
    """Return True if the specified file is small enough to process in memory."""
    size_in_mib = os.stat(path).st_size / 1e6
    return (_is_gzipped(path) and size_in_mib < 10) or size_in_mib < 100


def _is_gzipped(path):
    """Returns True if the specified path is a gzipped file."""
    with open(path, 'rb') as fin:
        return fin.read(len(_GZIP_MAGIC)) == _GZIP_MAGIC


def _split_large_file(path, lines_per_part=_LINES_PER_PART):
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

    tmpdir = tempfile.mkdtemp(prefix='csvi-')
    prefix = P.join(tmpdir, 'parts')
    os.mkdir(prefix)

    #
    # We do this here because it's simpler.  Afterwards, multiple threads will
    # require this directory to exist.
    #
    os.mkdir(P.join(tmpdir, 'columns'))

    split_exe = _get_exe('gsplit', 'split')
    split_flags = ['--filter', "%s > $FILE.gz" % gzip_exe,
                   '--lines=%s' % lines_per_part, '-', prefix + '/']
    split_command = plumbum.local[split_exe][split_flags]

    if _is_gzipped(path):
        chain = gzip_command | tail_command | split_command
    else:
        chain = cat_command | tail_command | split_command

    _LOGGER.debug('chain: %s', chain)
    chain()

    full_paths_to_parts = [P.join(prefix, f) for f in os.listdir(prefix)]
    _LOGGER.debug('full_paths_to_parts: %r', full_paths_to_parts)
    return full_paths_to_parts


def _get_exe(*preference):
    """Return the first available executable in preference."""
    for exe in preference:
        path = distutils.spawn.find_executable(exe)
        if path:
            return path


def _split_file(header, path, dialect=None, list_columns=None, list_separator=None,
                encoding='utf-8'):
    """Split a CSV file into columns, one column per file.

    :arg str header: The names for each column of the file.
    :arg str path: The full path to the file.
    :arg Dialect dialect: The CSV dialect to use when parsing.
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
    assert dialect
    assert list_separator

    with _open_for_reading(path) as fin:
        reader = csv.reader(fin, dialect=dialect)
        return split.split(header, reader, list_columns=list_columns,
                           list_separator=list_separator, path=path)


def _process_multi(header, paths, dialect, args):
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
        _split_file, header, dialect=dialect,
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
        compress_temporary=True, num_subprocesses=1,
        most_common=args.most_common,
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


def _concatenate(paths, batch_size=_MAX_ARGS):
    """Concatenate the specified files together into a single file.

    It's possible that paths contains more than what we can pass in a
    single subprocess call, so we split them into batches and append
    each batch individually.

    :param iterator paths: The paths to concatenate
    :param int batch_size: The max number of files to pass to a single cat call
    :returns: The new file path
    :rtype: str
    """
    handle, path = tempfile.mkstemp()
    os.close(handle)

    for batch in split.make_batches(paths, batch_size=_MAX_ARGS):
        with open(path, 'ab') as fout:
            command = ['cat'] + batch
            _LOGGER.debug('command: %r', command)
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


if __name__ == "__main__":
    main()
