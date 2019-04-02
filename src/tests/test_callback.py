#!/usr/bin/env python3

"""\
Test the ElasticCallback class.
"""

import collections

import pytest

from conftest import tdatafile
from databroker_elasticsearch import callback_from_name

# Ignore YAMLLoadWarning from databroker package
pytestmark = pytest.mark.filterwarnings('ignore:calling yaml::databroker[.]')

cb_config_file = tdatafile('dbes.yml')


def indexcount(cb):
    "Return number of entries in ES index attached to ElasticCallback."
    es = cb.esindex.es
    es.indices.refresh()
    s = es.cat.count(cb.esindex.index, h='count')
    return int(s)


def indexproperties(cb):
    "Retrieve mapping properties in ES index attached to ElasticCallback."
    es = cb.esindex.es
    ei = cb.esindex
    res = es.indices.get_mapping(ei.index, ignore_unavailable=True)
    rv = res[ei.index]['mappings'][ei.doc_type]['properties']
    return rv


def require_pi(doc):
    # only accept measurements by Mingzhao
    rv = (doc.get('PI', '') == 'Mingzhao')
    return rv


@pytest.fixture
def cb(es):
    "Create callback instance from config file, but use our Elasticsearch."
    cb = callback_from_name(tdatafile('dbes.yml'))
    cb.esindex.es = es
    cb.esindex.reset()
    return cb


@pytest.mark.parametrize('criteria,count', [(None, 3), (require_pi, 2)])
def test_callback_start(cb, criteria, count, issrecords):
    cb.esindex.criteria = criteria
    assert indexcount(cb) == 0
    for doc in issrecords:
        cb("start", doc)
    assert indexcount(cb) == count
    return


@pytest.mark.parametrize('criteria,count', [(None, 3), (require_pi, 2)])
def test_callback_rebuild(cb, criteria, count, issrecords):
    cb.esindex.criteria = criteria
    # add 1 dummy document
    cb.start({"_id": 1, "PI": "Mingzhao"})
    assert indexcount(cb) == 1
    # create a mock Header type with "start" attribute
    Header = collections.namedtuple('Header', 'start')
    headers = [Header(start=doc) for doc in issrecords]
    cb.rebuild(headers, purge=True)
    assert indexcount(cb) == count
    # check rebuild without purge
    cb.rebuild([], purge=False)
    assert indexcount(cb) == count
    cb.rebuild([], purge=True)
    assert indexcount(cb) == 0
    for i in range(len(headers)):
        cb.rebuild(headers[i:i + 1])
    assert indexcount(cb) == count
    return


def test_callback_new_index(cb, issrecords):
    es = cb.esindex.es
    ei = cb.esindex
    ei.index = "dbes-test-cbni"
    assert not es.indices.exists(ei.index)
    cb("start", issrecords[0])
    assert indexproperties(cb)['time'] == ei.doc_properties['time']
    assert indexcount(cb) == 1
    # test cb.rebuild with new index name
    ei.index = "dbes-test-cbni-2"
    assert not es.indices.exists(ei.index)
    # create a mock Header type with "start" attribute
    Header = collections.namedtuple('Header', 'start')
    headers = [Header(start=doc) for doc in issrecords]
    cb.rebuild(headers)
    assert indexproperties(cb)['time'] == ei.doc_properties['time']
    assert indexcount(cb) == 3
    return
