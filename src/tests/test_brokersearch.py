#!/usr/bin/env python3

"""\
Test the BrokerSearch class.
"""

import pytest

from conftest import tdatafile
from databroker_elasticsearch import callback_from_name
from databroker_elasticsearch.brokersearch import BrokerSearch

# Ignore YAMLLoadWarning from databroker package
pytestmark = pytest.mark.filterwarnings('ignore:calling yaml::databroker[.]')

cb_config_file = tdatafile('dbes.yml')


def ilength(ii):
    "Return number of items in an iterable `ii`."
    n = sum(1 for _ in ii)
    return n


@pytest.fixture(scope='module')
def bsearch(es, db, issrecords):
    "Create BrokerSearch instance filled with issrecords."
    cb = callback_from_name(cb_config_file)
    cb.esindex.es = es
    cb.esindex.index = 'dbes-test-brokersearch'
    for doc in issrecords:
        db.insert('start', doc)
    cb.rebuild(db())
    es.indices.refresh()
    rv = BrokerSearch(db, cb.esindex)
    return rv


def test_bksearch_all(bsearch, issrecords):
    cntall = len(issrecords)
    assert ilength(bsearch()) == cntall
    assert ilength(bsearch('*')) == cntall
    assert ilength(bsearch(query={'query': {'match_all': {}}})) == cntall
    return


def test_bksearch_q(bsearch):
    headers = list(bsearch('Kisa'))
    assert len(headers) == 1
    h = headers[0]
    assert h.start['scan_id'] == 47797
    assert h.start['PROPOSAL'] == '302753'
    return


def test_bksearch_date(bsearch, issrecords):
    cntall = len(issrecords)
    assert ilength(bsearch('date:2018-02-10')) == 1
    assert ilength(bsearch('date:2000-01-01')) == 0
    assert ilength(bsearch('date:[2000-01-01 TO *]')) == cntall
    res = bsearch(query={'query': {'range': {'scan_id': {'gt': 42500}}}})
    assert ilength(res) == 2
    return
