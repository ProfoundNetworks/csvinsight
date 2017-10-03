import io
import os.path as P

import csvinsight.cli

CURR_DIR = P.dirname(P.abspath(__file__))


def test_main():
    with open(P.join(CURR_DIR, 'expected.txt'), 'rb') as fin:
        expected = fin.read()
    csv = b"""\
name|age|fave_color
Misha|33|red;yellow
Valya|31|blue
Lyosha|0|
"""
    stdin = io.BytesIO(csv)
    stdout = io.BytesIO()
    csvinsight.cli.main(argv=['--subprocesses=1', '--list-fields', 'fave_color'],
                        stdin=stdin, stdout=stdout)
    assert stdout.getvalue() == expected
