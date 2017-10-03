#
# -*- coding: utf-8 -*-
# (C) Copyright: Profound Networks, LLC 2017
#

"""Console script for csvinsight."""
from __future__ import print_function

import argparse
import csv
import json
import logging
import multiprocessing
import os
import sys
import yaml

import six

from . import split
from . import summarize

_LOGGER = logging.getLogger(__name__)


def _add_default_args(parser):
    parser.add_argument('--loglevel', default=logging.INFO)


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


def _process_full(reader, args):
    header, histogram, paths = split.split(
        reader, list_columns=args.list_fields, list_separator=args.list_separator
    )

    pool = multiprocessing.Pool(processes=args.subprocesses)
    results = pool.map(summarize.sort_and_summarize, paths)

    for path in paths:
        os.unlink(path)

    return header, histogram, results


def _override_config(fin, args):
    config = yaml.load(fin)
    args.delimiter = config.get('delimiter', args.delimiter)
    args.list_separator = config.get('list_separator', args.list_separator)
    args.list_fields = config.get('list_fields', args.list_fields)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--subprocesses', type=int, default=multiprocessing.cpu_count())
    parser.add_argument('--file', default=None)
    parser.add_argument('--config', default=None)
    parser.add_argument('--quick', action='store_true')
    _add_default_args(parser)
    _add_map_args(parser)
    args = parser.parse_args()

    logging.basicConfig(level=args.loglevel)

    if args.file:
        stream = open(args.file)
    else:
        stream = sys.stdin

    if args.config:
        with open(args.config) as fin:
            _override_config(fin, args)
    _LOGGER.info('args: %r', args)

    reader = csv.reader(stream, delimiter=args.delimiter, quoting=csv.QUOTE_NONE,
                        escapechar='')
    if args.quick:
        raise NotImplementedError
    else:
        header, histogram, results = _process_full(reader, args)

    _print_header(histogram, sys.stdout)
    for number, (name, result) in enumerate(zip(header, results), 1):
        result.update(number=number, name=name)
        _print_column_summary(result, sys.stdout)


def main_split():
    parser = argparse.ArgumentParser()
    _add_default_args(parser)
    _add_map_args(parser)
    args = parser.parse_args()

    logging.basicConfig(level=args.loglevel)

    reader = csv.reader(sys.stdin, delimiter=args.delimiter, quoting=csv.QUOTE_NONE,
                        escapechar='')
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

    summary = summarize.summarize_sorted(line.rstrip(b'\n') for line in sys.stdin)
    print(json.dumps(summary) + b'\n')


if __name__ == "__main__":
    main()
