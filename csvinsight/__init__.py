# -*- coding: utf-8 -*-

"""Top-level package for csvinsight."""

import smart_open.compression
import zstandard

__author__ = """Michael Penkov"""
__email__ = 'm@penkov.dev'
__version__ = '0.3.3'


def _handle_zst(file_obj, mode):
    result = zstandard.open(filename=file_obj, mode=mode)
    return result


smart_open.compression.register_compressor('.zst', _handle_zst)
