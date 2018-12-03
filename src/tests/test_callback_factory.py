#!/usr/bin/env python3

"""\
Test the callback_from_config() factory method.
"""

import os.path

import pytest
import yaml

from databroker_elasticsearch import callback_from_config
from databroker_elasticsearch import callback_from_name
from conftest import tdatafile

@pytest.fixture()
def tweak_databroker_search_path():
    import databroker._core as dbcore
    cspsave = dbcore.CONFIG_SEARCH_PATH
    dbcore.CONFIG_SEARCH_PATH = (tdatafile(''),)
    yield
    dbcore.CONFIG_SEARCH_PATH = cspsave
    return


def test_callback_from_config():
    f = tdatafile('dbes.yml')
    with open(f) as fp:
        cfg = yaml.load(fp)
    cb = callback_from_config(cfg)
    assert cb.esindex.index == 'dbes-test-iss'
    doc1 = {'_id': 13, 'SAF': 1234, 'year': '2018'}
    edoc1 = cb.esindex.mapper(doc1)
    assert len(edoc1) == 3
    assert edoc1['_id'] == "13"
    assert edoc1['saf'] == 1234
    assert edoc1['year'] == 2018
    return


@pytest.mark.usefixtures('tweak_databroker_search_path')
def test_callback_from_name():
    f = os.path.realpath(tdatafile('dbes.yml'))
    cb0 = callback_from_name(f)
    assert cb0.esindex.index == 'dbes-test-iss'
    cb1 = callback_from_name('dbes')
    assert cb1.esindex.index == 'dbes-test-iss'
    with pytest.raises(FileNotFoundError):
        callback_from_name('does-not-exist')
    return
