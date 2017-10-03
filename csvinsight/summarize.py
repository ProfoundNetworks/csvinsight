"""Summarize a single column of values."""
from __future__ import division
import pipes
import sys


BLANK = b''


def run_length_encode(iterator):
    run_value, run_length = next(iterator), 1
    for value in iterator:
        if value < run_value:
            raise ValueError('unsorted iterator')
        elif value != run_value:
            yield run_value, run_length
            run_value, run_length = value, 1
        else:
            run_length += 1
    yield run_value, run_length


def summarize_sorted(iterator):
    num_values = 0
    num_uniques = 0
    num_empty = 0
    max_len = 0
    min_len = sys.maxint
    sum_len = 0

    for run_value, run_length in run_length_encode(iterator):
        if run_value == BLANK:
            num_empty = run_length
        num_values += run_length
        num_uniques += 1
        val_len = len(run_value)
        max_len = max(max_len, val_len)
        min_len = min(min_len, val_len)
        sum_len += val_len * run_length

    return {
        'num_values': num_values,
        'num_fills': num_values - num_empty,
        'fill_ratio': (num_values - num_empty) / num_values,
        'max_len': max_len,
        'min_len': min_len,
        'avg_len': sum_len / num_values,
        'num_uniques': num_uniques,
    }


def summarize_unsorted(iterator):
    num_values = 0
    num_empty = 0
    max_len = 0
    min_len = sys.maxint
    sum_len = 0

    for value in iterator:
        if value == BLANK:
            num_empty += 1
        num_values += 1
        val_len = len(value)
        max_len = max(max_len, val_len)
        min_len = min(min_len, val_len)
        sum_len += val_len

    return {
        'num_values': num_values,
        'num_fills': num_values - num_empty,
        'fill_ratio': (num_values - num_empty) / num_values,
        'max_len': max_len,
        'min_len': min_len,
        'avg_len': sum_len / num_values,
    }


def sort_and_summarize(path):
    template = pipes.Template()
    template.append('LC_ALL=C sort', '--')
    with template.open(path, 'r') as fin:
        result = summarize_sorted(line.rstrip(b'\n') for line in fin)
    return result
