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


@pytest.mark.xfail
def test_bksearch_all(bsearch):
    # assert ilength(bsearch()) == 3
    assert ilength(bsearch('*')) == 3
    assert ilength(bsearch(query={'query': {'match_all': {}}})) == 3
    return
