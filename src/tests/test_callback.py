#!/usr/bin/env python3

"""\
Test the ElasticCallback class.
"""

import pytest

import collections
import json

from conftest import tdatafile
from databroker_elasticsearch import callback_from_name


cb_config_file = tdatafile('dbes.yml')


def issrecords():
    with open(tdatafile('iss-sample.json')) as fp:
        rv = json.load(fp)
    return rv


def indexcount(cb):
    "Return number of entries in ES index attached to ElasticCallback."
    es = cb.esindex.es
    es.indices.refresh()
    s = es.cat.count(cb.esindex.index, h='count')
    return int(s)


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
def test_callback_start(cb, criteria, count):
    cb.esindex.criteria = criteria
    assert indexcount(cb) == 0
    for doc in issrecords():
        cb("start", doc)
    assert indexcount(cb) == count
    return


@pytest.mark.parametrize('criteria,count', [(None, 3), (require_pi, 2)])
def test_callback_rebuild(cb, criteria, count):
    cb.esindex.criteria = criteria
    # add 1 dummy document
    cb.start({"_id": 1, "PI": "Mingzhao"})
    assert indexcount(cb) == 1
    # create a mock callable with databroker-like return
    Header = collections.namedtuple('Header', 'start')
    dbmock = lambda: (Header(start=doc) for doc in issrecords())
    cb.rebuild(dbmock)
    assert indexcount(cb) == count
    return
