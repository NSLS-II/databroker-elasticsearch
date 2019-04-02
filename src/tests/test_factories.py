#!/usr/bin/env python3

"""\
Test factory functions in top package namespace.
"""

import pytest
import yaml

from databroker_elasticsearch import callback_from_config
from databroker_elasticsearch import callback_from_name
from databroker_elasticsearch import load_callback
from databroker_elasticsearch import load_elasticindex

from conftest import tdatafile

# Ignore YAMLLoadWarning from databroker package
pytestmark = pytest.mark.filterwarnings('ignore:calling yaml::databroker[.]')


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
        cfg = yaml.safe_load(fp)
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
    cb0 = load_callback(tdatafile('dbes.yml'))
    assert cb0.esindex.index == 'dbes-test-iss'
    cb1 = callback_from_name('dbes')
    assert cb1.esindex.index == 'dbes-test-iss'
    with pytest.raises(FileNotFoundError):
        callback_from_name('does-not-exist')
    return


def test_load_elasticindex():
    eidx = load_elasticindex(tdatafile('dbes.yml'))
    assert eidx.index == 'dbes-test-iss'
    assert len(eidx.mapper.docmap) == 21
    assert all((len(tp) == 3) for tp in eidx.mapper.docmap)
    return
