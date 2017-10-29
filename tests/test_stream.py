import csvinsight.stream


def test_column():
    column = csvinsight.stream.Column()
    column.add('bar')
    column.extend(('bar', 'baz', 'boz', ''))
    expected = {
        'num_values': 5, 'num_fills': 4, 'fill_rate': 80.,
        'max_len': 3, 'min_len': 0, 'avg_len': 12./5,
        'num_uniques': -1, 'most_common': [],
    }
    actual = column.summarize()
    assert actual == expected
