from __future__ import unicode_literals

import io
import os.path as P

import six

import csvinsight.cli

CURR_DIR = P.dirname(P.abspath(__file__))


def test_main():
    mode = 'rb' if six.PY2 else 'r'
    with open(P.join(CURR_DIR, 'expected.txt'), mode) as fin:
        expected = fin.read()
    csv = """\
name|age|fave_color
Misha|33|red;yellow
Valya|31|blue
Lyosha|0|
"""
    stdin = io.StringIO(csv)
    stdout = io.StringIO()
    csvinsight.cli.main(argv=['--subprocesses=1', '--list-fields', 'fave_color'],
                        stdin=stdin, stdout=stdout)
    assert stdout.getvalue() == expected
