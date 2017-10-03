import pytest

import csvinsight.summarize


def test_summarize_sorted():
    column = iter(('', '1', '2', '2', '3', '3', '3', 'aa'))
    expected = {
        'num_values': 8,
        'num_fills': 7,
        'fill_rate': 700./8,
        'max_len': 2,
        'min_len': 0,
        'avg_len': 1.0,
        'num_uniques': 5,
        'most_common': [(3, '3'), (2, '2'), (1, 'aa'), (1, '1'), (1, '')],
    }
    assert csvinsight.summarize.summarize_sorted(column) == expected


def test_run_length_encode():
    expected = [(1, 1), (2, 2), (3, 3)]
    actual = list(csvinsight.summarize.run_length_encode(iter([1, 2, 2, 3, 3, 3])))
    assert expected == actual

    with pytest.raises(ValueError):
        list(csvinsight.summarize.run_length_encode(iter([2, 1])))


def test_top_n():
    topn = csvinsight.summarize.TopN(limit=3)
    topn.push(1, 'foo')
    topn.push(2, 'bar')
    topn.push(3, 'baz')
    assert topn.to_list() == [(1, 'foo'), (2, 'bar'), (3, 'baz')]

    topn.push(4, 'boz')
    assert topn.to_list() == [(2, 'bar'), (3, 'baz'), (4, 'boz')]

    topn.push(1, 'oops')
    assert topn.to_list() == [(2, 'bar'), (3, 'baz'), (4, 'boz')]
