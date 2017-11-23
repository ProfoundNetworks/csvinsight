from __future__ import unicode_literals

import io
import os.path as P

import mock
import six

import csvinsight.cli

CURR_DIR = P.dirname(P.abspath(__file__))


def test_main():
    pass


def test_run_in_memory():
    reader = iter([('name', 'age', 'fave_color'),
                   ('Misha', '33', 'red;yellow'),
                   ('Valya', '31', 'blue'),
                   ('Lyosha', 0)])
    args = mock.Mock(list_fields=['fave_color'], list_separator=';')
    header, histogram, column_summaries = csvinsight.cli._run_in_memory(reader, args)
    assert header == ('name', 'age', 'fave_color')
    assert dict(histogram) == {3: 2, 2: 1}
    assert len(column_summaries) == 3
