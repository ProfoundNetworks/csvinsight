from __future__ import unicode_literals

import io

import six
import six.moves.queue as Queue

import csvinsight.split


def mock_batch():
    if six.PY2:
        yield [b'foo', b'bar']
        yield [b'baz']
    else:
        yield ['foo', 'bar']
        yield ['baz']
    yield csvinsight.split.SENTINEL


def test_writer_thread():
    buf = io.BytesIO()
    buf.close = lambda: None

    def open_temp_file(mode):
        return buf, '/some/dummy/path'

    queue = Queue.Queue()
    for batch in mock_batch():
        queue.put(batch)

    thread = csvinsight.split.WriterThread(queue, open_temp_file)
    thread.start()
    thread.join()

    expected = b'foo\nbar\nbaz\n'
    assert buf.getvalue() == expected


def test_make_batches():
    assert list(csvinsight.split.make_batches([1, 2], 1)) == [[1], [2]]
    assert list(csvinsight.split.make_batches([1, 2], 2)) == [[1, 2]]
    assert list(csvinsight.split.make_batches([1, 2, 3], 2)) == [[1, 2], [3]]
