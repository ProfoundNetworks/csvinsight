import collections
import io
import queue

import pytest

import csvinsight.split


def mock_batch():
    yield ['foo', 'bar']
    yield ['baz']
    yield csvinsight.split.SENTINEL


def test_writer_thread():
    buf = io.BytesIO()
    buf.close = lambda: None

    def open_temp_file(subdir, column_id):
        return buf, '/%s/%04d.gz' % (subdir, column_id)

    q = queue.Queue()
    for batch in mock_batch():
        q.put(batch)

    thread = csvinsight.split.WriterThread(
        '/tmp/subdir', 0, q, open_temp=open_temp_file
    )
    thread.start()
    thread.join()

    expected = b'foo\nbar\nbaz\n'
    assert buf.getvalue() == expected


def test_make_batches():
    assert list(csvinsight.split.make_batches([1, 2], 1)) == [[1], [2]]
    assert list(csvinsight.split.make_batches([1, 2], 2)) == [[1, 2]]
    assert list(csvinsight.split.make_batches([1, 2, 3], 2)) == [[1, 2], [3]]


def test_run_in_memory():
    reader = [
        ('foo', 'bar', 'baz'),
        ('1', '2', '3'),
        ('0', 'a;b', ''),
        ('', ''),
    ]
    header, histogram, columns = csvinsight.split.split_in_memory(
        iter(reader), list_columns=('bar',)
    )
    assert header == ('foo', 'bar', 'baz')
    assert histogram == collections.Counter([3, 3, 2])
    assert columns == [['1', '0'], ['2', 'a', 'b'], ['3', '']]


def test_read_empty_file():
    with pytest.raises(ValueError):
        csvinsight.split.split_in_memory(iter([]))


def test_populate_queues():
    header = ('value', 'list')
    reader = [('123', 'a;b;c'), ('456', 'd;e;f'), ('789', 'g;h;i')]
    queues = (queue.Queue(), queue.Queue())
    csvinsight.split._populate_queues(header, reader, queues,
                                      list_columns=['list'], batch_size=2)

    #
    # Each list contains batches.
    #
    first = list(read_queue(queues[0]))
    assert first == [['123', '456'], ['789']]

    second = list(read_queue(queues[1]))
    assert second == [['a', 'b', 'c', 'd', 'e', 'f'], ['g', 'h', 'i']]


def read_queue(q):
    while True:
        item = q.get()
        if item == csvinsight.split.SENTINEL:
            break
        yield item
