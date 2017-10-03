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

from . import csvinsight
from . import split
from . import summarize

_LOGGER = logging.getLogger(__name__)


def _add_default_args(parser):
    parser.add_argument('--loglevel', default=logging.INFO)


def _add_map_args(parser):
    parser.add_argument('--delimiter', default='|')
    parser.add_argument('--list-separator', default=';')
    parser.add_argument('--list-fields', nargs='*', default=[])


def main_map():
    parser = argparse.ArgumentParser()
    _add_default_args(parser)
    _add_map_args(parser)
    args = parser.parse_args()

    logging.basicConfig(level=args.loglevel)
    header, counter, output_dir = csvinsight.map(sys.stdin, list_fields=args.list_fields)
    print(output_dir)


def main_reduce():
    parser = argparse.ArgumentParser()
    _add_default_args(parser)
    args = parser.parse_args()

    logging.basicConfig(level=args.loglevel)
    summary = csvinsight.reduce(sys.stdin)
    #
    # This can convert UTF-8 to Unicode, e.g.
    # Agora Solu\xc3\xa7\xc3\xb5es -> Agora Solu\u00e7\u00f5es
    #
    json.dump(summary, sys.stdout)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--subprocesses', type=int, default=multiprocessing.cpu_count())
    _add_default_args(parser)
    _add_map_args(parser)
    args = parser.parse_args()

    logging.basicConfig(level=args.loglevel)

    reader = csv.reader(sys.stdin, delimiter=args.delimiter, quoting=csv.QUOTE_NONE,
                        escapechar='')
    header, histogram, paths = split.split(
        reader, list_columns=args.list_fields, list_separator=args.list_separator
    )

    sys.stdout.write(json.dumps(histogram, sort_keys=True) + '\n')

    pool = multiprocessing.Pool(processes=args.subprocesses)
    results = pool.map(summarize.sort_and_summarize, paths)

    for column, path, result in zip(header, paths, results):
        result['_id'] = column
        sys.stdout.write(json.dumps(result, sort_keys=True) + '\n')
        os.unlink(path)


def main_split():
    parser = argparse.ArgumentParser()
    parser.add_argument('--delimiter', default='|')
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


if __name__ == "__main__":
    main()
