#
# -*- coding: utf-8 -*-
# (C) Copyright: Profound Networks, LLC 2017
#

"""Console script for csvinsight."""
from __future__ import print_function

import argparse
import logging
import sys

import json

from . import csvinsight

_LOGGER = logging.getLogger(__name__)


def _add_default_args(parser):
    parser.add_argument('--loglevel', default=logging.INFO)


def _add_map_args(parser):
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
    """Main console script for csvinsight."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--simple', action='store_true')
    _add_default_args(parser)
    _add_map_args(parser)
    args = parser.parse_args()

    logging.basicConfig(level=args.loglevel)

    map_kwargs = {'list_fields': args.list_fields}
    if args.simple:
        report = csvinsight.simple_report(sys.stdin, **map_kwargs)
    else:
        report = csvinsight.full_report(sys.stdin, map_kwargs=map_kwargs)
    _LOGGER.info('finished reduce, formatting report')
    csvinsight.print_report(report, sys.stdout)


if __name__ == "__main__":
    main()
