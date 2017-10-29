#
# -*- coding: utf-8 -*-
# (C) Copyright: Profound Networks, LLC 2017
#

"""Console script for csvinsight."""
from __future__ import print_function
from __future__ import unicode_literals

import argparse
import csv
import json
import logging
import multiprocessing
import os
import sys
import tempfile
import yaml

import six

from . import split
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


def parse_args(args):
    parser = argparse.ArgumentParser()
    parser.add_argument('--subprocesses', type=int, default=multiprocessing.cpu_count())
    parser.add_argument('--file', default=None)
    parser.add_argument('--config', default=None)
    parser.add_argument('--quick', action='store_true')
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
        stream = open(args.file)
    else:
        stream = stdin

    if args.config:
        with open(args.config) as fin:
            _override_config(fin, args)
    _LOGGER.info('args: %r', args)

    reader = _open_csv(stream, delimiter=args.delimiter)
    if args.quick:
        raise NotImplementedError
    elif args.in_memory:
        header, histogram, results = _run_in_memory(reader, args)
    else:
        header, histogram, results = _process_full(reader, args)

    _print_header(histogram, stdout)
    for number, (name, result) in enumerate(zip(header, results), 1):
        result.update(number=number, name=name)
        _print_column_summary(result, stdout)


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
