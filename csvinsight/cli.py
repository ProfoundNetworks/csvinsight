#
# -*- coding: utf-8 -*-
# (C) Copyright: Profound Networks, LLC 2017
#

"""Console script for csvinsight."""

import argparse
import copy
import csv
import distutils.spawn
import contextlib
import heapq
import json
import logging
import multiprocessing
import os
import shutil
import subprocess
import sys
import tempfile

import smart_open
import yaml

_LOGGER = logging.getLogger(__name__)

LIST_SEPARATOR = ';'
MOST_COMMON = 100

_CSV_DIALECT_PARAMS = {
    'delimiter': '|',
    'quotechar': '\'',
    'escapechar': '\\',
    'doublequote': '"',
    'skipinitialspace': 'True',
    'lineterminator': '\n',
    'quoting': 'QUOTE_NONE'
}


def _print_report(header, histogram, results, fout=sys.stdout):
    _print_header(histogram, fout)
    for number, (name, result) in enumerate(zip(header, results), 1):
        result.update(number=number, name=name)
        _print_column_summary(result, fout)


def _print_header(histogram, fout):
    with contextlib.redirect_stdout(fout):
        print('CSV Insight Report')
        print(f'Total # Rows: {sum(histogram.values())}')
        print('Column counts:', file=fout)
        for num_col, freq in sorted(
            histogram.items(),
            key=lambda item: item[1],
            reverse=True
        ):
            print(f'        {num_col}  columns ->  {freq} rows')
        print()


def _print_column_summary(summary, fout):
    with contextlib.redirect_stdout(fout):
        number = summary['number']
        name = summary['name']
        num_uniques = summary['num_uniques']
        num_fills = summary['num_fills']
        fill_rate = summary['fill_rate']
        min_len = summary['min_len']
        max_len = summary['max_len']
        avg_len = summary['avg_len']

        print(
            f'{number:3}. {name} -> Uniques: {num_uniques}; '
            f'Fills: {num_fills} ; Fill Rate: {fill_rate:.1f}%'
        )
        print(f'       Field Length:  min {min_len}, max {max_len}, avg {avg_len:.2f}')

        if num_uniques == -1:
            print('')
            return

        num_samples = remainder = summary['num_values']
        print(f"{'Counts':>10}  {'Percent':>12}  Field Value", file=fout)
        for count, value in summary['most_common']:
            if value == '':
                value = 'NULL'
            print(f'{count:10}  {count * 100.0 / num_samples:10.2f} %  {value}')
            remainder -= count

        if remainder:
            print(f'{count:10}  {count * 100.0 / num_samples:10.2f} %  Other')
        print('')


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
    kwargs = dict(pair.split('=', 1) for pair in pairs_as_strings)
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


def _get_exe(*preference):
    """Return the first available executable in preference."""
    for exe in preference:
        path = distutils.spawn.find_executable(exe)
        if path:
            return path


def _split(reader, list_columns=[], list_separator=LIST_SEPARATOR):
    tmpdir = tempfile.mkdtemp()
    header = next(reader)
    list_column_indices = {header.index(column) for column in list_columns}
    fouts = [
        open(os.path.join(tmpdir, '%04d' % i), 'wt')
        for i, _ in enumerate(header)
    ]

    histogram = {}
    for row in reader:
        try:
            histogram[len(row)] += 1
        except KeyError:
            histogram[len(row)] = 1

        if len(row) != len(header):
            #
            # Malformed row, don't evaluate it
            #
            continue

        for colindex, colvalue in enumerate(row):
            if colindex in list_column_indices:
                split = colvalue.split(LIST_SEPARATOR)
                for x in split:
                    print(x, file=fouts[colindex])
            else:
                print(colvalue, file=fouts[colindex])

    for f in fouts:
        f.flush()
        f.close()

    return header, histogram, tmpdir


def _sort(path, buffer_size='2G', num_subprocesses=0, num_most_common=MOST_COMMON):
    if num_subprocesses == 0:
        num_subprocesses = multiprocessing.cpu_count()

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        command = [
            _get_exe('gsort', 'sort'),
            f'--parallel={num_subprocesses}',
            f'--buffer-size={buffer_size}',
            path,
        ]
        try:
            subprocess.check_call(command, stdout=tmp, env={'LC_ALL': 'C'})
        except Exception:
            os.unlink(tmp.name)
        else:
            os.rename(tmp.name, path)


def _run_length_encode(iterator):
    try:
        run_value, run_length = next(iterator), 1
        run_value = run_value.rstrip('\n')
    except StopIteration:
        #
        # Empty iterator, nothing to do.
        #
        pass
    else:
        for value in iterator:
            value = value.rstrip()
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


def _tally(path, most_common):
    num_values = 0
    num_uniques = 0
    num_empty = 0
    max_len = 0
    min_len = sys.maxsize
    sum_len = 0
    topn = TopN(limit=most_common)

    with open(path, 'rt') as fin:
        for run_value, run_length in _run_length_encode(fin):
            if len(run_value) == 0:
                num_empty = run_length
            num_values += run_length
            num_uniques += 1
            val_len = len(run_value)
            max_len = max(max_len, val_len)
            min_len = min(min_len, val_len)
            sum_len += val_len * run_length
            topn.push(run_length, run_value)

        if num_values == 0:
            raise ValueError('CSV file contains no data')

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
        description=description,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=epilog
    )
    parser.add_argument(
        'path',
        default=None,
        help='The path to the CSV file to open',
    )
    parser.add_argument(
        '--loglevel',
        choices=log_levels,
        default=logging.INFO,
    )
    parser.add_argument(
        '--subprocesses',
        type=int,
        default=multiprocessing.cpu_count(),
        metavar='NUM_CORES',
        help='The number of subprocesses to use',
    )
    parser.add_argument(
        '--config',
        default=None,
        metavar='PATH',
        help='The path to the configuration file.',
    )
    parser.add_argument(
        '--dialect',
        nargs='+',
        default=[],
        metavar='KEY=VALUE',
        help='The CSV dialect to use when parsing the file',
    )
    parser.add_argument(
        '--list-fields',
        nargs='*',
        default=[],
        metavar='FIELD_NAME',
        help='The names of fields that contain lists instead of atomic values',
    )
    parser.add_argument(
        '--list-separator',
        default=';',
        metavar='CHARACTER',
        help='The separator used to split lists into atomic values',
    )
    parser.add_argument(
        '--most-common',
        default=MOST_COMMON,
        type=int,
        help='The number of most common values to show for each column',
    )
    parser.add_argument(
        '--json',
        help='Write a JSON version of the report to this file'
    )
    parser.add_argument('--tempdir', help='Where to write temporary files to')
    args = parser.parse_args()

    logging.basicConfig(level=args.loglevel)
    for lgr in ('botocore', 'boto3'):
        logging.getLogger(lgr).setLevel(logging.ERROR)
    tempfile.tempdir = args.tempdir

    if args.config:
        with open(args.config) as fin:
            _override_config(fin, args)

    csv_dialect = _parse_dialect(args.dialect)

    results = []
    with smart_open.open(args.path, 'rt') as fin:
        reader = csv.reader(fin, dialect=csv_dialect)
        header, histogram, tempdir = _split(reader, args.list_fields)
        try:
            for i, filename in enumerate(sorted(os.listdir(tempdir))):
                path = os.path.join(tempdir, filename)
                _sort(path)
                r = _tally(path, args.most_common)
                results.append(r)
        finally:
            shutil.rmtree(tempdir)

    report_as_json = {
        'path': args.path,
        'histogram': histogram,
        'header': header,
        'results': results,
    }

    if args.json:
        with open(args.json, 'wt') as fout:
            json.dump(report_as_json, fout)

    _print_report(header, histogram, results, fout=sys.stdout)


if __name__ == "__main__":
    main()
