from __future__ import unicode_literals

import collections
import io

import pytest
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

    def open_temp_file(mode, prefix='tmp'):
        return buf, '/some/dummy/path'

    queue = Queue.Queue()
    for batch in mock_batch():
        queue.put(batch)

    thread = csvinsight.split.WriterThread(0, 0, queue, open_temp_file)
    thread.start()
    thread.join()

    expected = b'foo\nbar\nbaz\n'
    assert buf.getvalue() == expected


def test_make_batches():
    assert list(csvinsight.split._make_batches([1, 2], 1)) == [[1], [2]]
    assert list(csvinsight.split._make_batches([1, 2], 2)) == [[1, 2]]
    assert list(csvinsight.split._make_batches([1, 2, 3], 2)) == [[1, 2], [3]]


def test_run_in_memory():
    reader = [
        (b'foo', b'bar', b'baz'),
        (b'1', b'2', b'3'),
        (b'0', b'a;b', b''),
        (b'', b''),
    ]
    header, histogram, columns = csvinsight.split.split_in_memory(
        iter(reader), list_columns=('bar',)
    )
    assert header == (b'foo', b'bar', b'baz')
    assert histogram == collections.Counter([3, 3, 2])
    assert columns == [[b'1', b'0'], [b'2', b'a', b'b'], [b'3', b'']]


def test_read_empty_file():
    with pytest.raises(ValueError):
        csvinsight.split.split_in_memory(iter([]))


def test_populate_queues():
    header = ('value', 'list')
    reader = [('123', 'a;b;c'), ('456', 'd;e;f'), ('789', 'g;h;i')]
    queues = (Queue.Queue(), Queue.Queue())
    csvinsight.split._populate_queues(header, reader, queues,
                                      list_columns=['list'], batch_size=2)

    #
    # Each list contains batches.
    #
    first = list(read_queue(queues[0]))
    assert first == [['123', '456'], ['789']]

    second = list(read_queue(queues[1]))
    assert second == [['a', 'b', 'c', 'd', 'e', 'f'], ['g', 'h', 'i']]


def read_queue(queue):
    while True:
        item = queue.get()
        if item == csvinsight.split.SENTINEL:
            break
        yield item
