# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import csv
import io
import os.path as P

import mock

import csvinsight.cli

CURR_DIR = P.dirname(P.abspath(__file__))


def test_main():
    pass


def test_run_in_memory():
    reader = iter([('name', 'age', 'fave_color'),
                   ('Misha', '33', 'red;yellow'),
                   ('Valya', '31', 'blue'),
                   ('Lyosha', 0)])
    args = mock.Mock(list_fields=['fave_color'], list_separator=';', most_common=20)
    header, histogram, column_summaries = csvinsight.cli._run_in_memory(reader, args)
    assert header == ('name', 'age', 'fave_color')
    assert dict(histogram) == {3: 2, 2: 1}
    assert len(column_summaries) == 3


def test_parse_dialect_delimiter():
    opts = ('delimiter=\t', 'quotechar=\'', 'escapechar=\\', 'doublequote="',
            'skipinitialspace=False', 'lineterminator=\n', 'quoting=QUOTE_ALL')
    dialect = csvinsight.cli._parse_dialect(opts)
    assert dialect.delimiter == '\t'
    assert dialect.quotechar == '\''
    assert dialect.escapechar == '\\'
    assert dialect.doublequote == '"'
    assert dialect.skipinitialspace is False
    assert dialect.quoting == csv.QUOTE_ALL


def test_print_column_summary():
    summary = dict(number=0, name='name', num_values=1, num_uniques=1,
                   num_fills=1, fill_rate=1, min_len=1, max_len=1, avg_len=1)
    summary['most_common'] = [(1, 'проверка')]
    fout = io.StringIO()
    csvinsight.cli._print_column_summary(summary, fout)
    expected = """\
0. name -> Uniques: 1 ; Fills: 1 ; Fill Rate: 1.0%
    Field Length:  min 1, max 1, avg 1.00
        Counts      Percent  Field Value
        1           100.00 %  проверка

"""
    assert fout.getvalue() == expected
