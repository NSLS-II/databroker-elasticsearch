#!/usr/bin/env python3

"""\
Test the ElasticIndex class.
"""

import pytest

from elasticsearch import Elasticsearch, NotFoundError
from databroker_elasticsearch.elasticindex import ElasticIndex


@pytest.fixture(scope='module')
def es():
    e = Elasticsearch()
    yield e
    e.indices.delete('dbes-test-*')
    return


def test___init__(es):
    ei = ElasticIndex(es, 'foo')
    assert isinstance(ei.es, Elasticsearch)
    assert ei.index == 'foo'
    assert ei.mapper is None
    assert ei.criteria is None
    return


def test_reset(es):
    ei = ElasticIndex(es, 'dbes-test-foo')
    bqall = {"query": {"match_all": {}}}
    with pytest.raises(NotFoundError):
        es.search(index=ei.index, body=bqall)
    ei.reset()
    res = es.search(index=ei.index, body=bqall)
    assert res['hits']['total'] == 0
    return
