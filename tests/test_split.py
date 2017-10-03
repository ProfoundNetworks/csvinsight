import io
import Queue

import csvinsight.split


def test_writer_thread():
    buf = io.BytesIO()

    queue = Queue.Queue()
    for batch in (['foo', 'bar'], ['baz'], csvinsight.split.SENTINEL):
        queue.put(batch)

    csvinsight.split.writer_thread(queue, buf)
    expected = b'foo\nbar\nbaz\n'
    assert buf.getvalue() == expected


def test_make_batches():
    assert list(csvinsight.split.make_batches([1, 2], 1)) == [[1], [2]]
    assert list(csvinsight.split.make_batches([1, 2], 2)) == [[1, 2]]
    assert list(csvinsight.split.make_batches([1, 2, 3], 2)) == [[1, 2], [3]]
