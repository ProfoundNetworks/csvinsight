import pytest

import csvinsight.summarize


def test_summarize_sorted():
    column = iter(('', '1', '2', '2', '3', '3', '3', 'aa'))
    expected = {
        'num_values': 8,
        'num_fills': 7,
        'fill_ratio': 7./8,
        'max_len': 2,
        'min_len': 0,
        'avg_len': 1.0,
        'num_uniques': 5
    }
    assert csvinsight.summarize.summarize_sorted(column) == expected


def test_run_length_encode():
    expected = [(1, 1), (2, 2), (3, 3)]
    actual = list(csvinsight.summarize.run_length_encode(iter([1, 2, 2, 3, 3, 3])))
    assert expected == actual

    with pytest.raises(ValueError):
        list(csvinsight.summarize.run_length_encode(iter([2, 1])))
