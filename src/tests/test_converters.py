#!/usr/bin/env python3

"""\
Test registered converter functions.
"""

import os
import time

import pytest

from databroker_elasticsearch.converters import getconverter


def test_simple_converters():
    assert 33 == getconverter('int')('33')
    assert 4.5 == getconverter('float')('0.45E+01')
    assert "12" == getconverter('str')(12)
    return


@pytest.fixture()
def use_est_timezone():
    tzsave = {'TZ': os.environ['TZ']} if 'TZ' in os.environ else {}
    os.environ['TZ'] = 'EST'
    time.tzset()
    yield
    # restore the TZ environment variable exactly as before
    del os.environ['TZ']
    os.environ.update(tzsave)
    time.tzset()
    return


@pytest.mark.usefixtures('use_est_timezone')
def test_toisoformat():
    fcnv = getconverter('toisoformat')
    ny2018noon = 1514826000
    assert fcnv(ny2018noon) == '2018-01-01T12:00:00'
    assert fcnv(ny2018noon + 0.1235) == '2018-01-01T12:00:00.124'
    return


def test_normalize_counts():
    f = getconverter('normalize_counts')
    assert f(99) is None
    assert f({'a': 1, 'b': 3}) == {'a': 0.25, 'b': 0.75}
    return


def test_listofstrings():
    f = getconverter('listofstrings')
    assert f(1) is None
    assert f([1, 2, 3]) is None
    words = 'one two three'.split()
    assert f(words) is words
    return
