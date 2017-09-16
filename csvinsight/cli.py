#
# -*- coding: utf-8 -*-
# (C) Copyright: Profound Networks, LLC 2017
#

"""Console script for csvinsight."""
from __future__ import print_function

import logging
import sys

import click
import json

from . import csvinsight

_LOGGER = logging.getLogger(__name__)


@click.command()
def main_map(args=None):
    logging.basicConfig(level=logging.INFO)
    header, counter, output_dir = csvinsight.map(sys.stdin)
    print(output_dir)


@click.command()
def main_reduce(args=None):
    logging.basicConfig(level=logging.INFO)
    summary = csvinsight.reduce(sys.stdin)
    #
    # This can convert UTF-8 to Unicode, e.g.
    # Agora Solu\xc3\xa7\xc3\xb5es -> Agora Solu\u00e7\u00f5es
    #
    json.dump(summary, sys.stdout)


@click.command()
def main(args=None):
    """Main console script for csvinsight."""
    logging.basicConfig(level=logging.INFO)
    report = csvinsight.generate_report(sys.stdin)
    _LOGGER.info('finished reduce, formatting report')
    csvinsight.print_report(report, sys.stdout)


if __name__ == "__main__":
    main()
