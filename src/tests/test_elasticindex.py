#!/usr/bin/env python3

"""\
Test the ElasticIndex class.
"""

import pytest

from elasticsearch import Elasticsearch, NotFoundError
from databroker_elasticsearch.elasticindex import ElasticIndex


def test___init__(es):
    ei = ElasticIndex(es, 'foo')
    assert ei.es is es
    assert ei.index == 'foo'
    assert ei.mapper is None
    assert ei.criteria is None
    ei1 = ElasticIndex('localhost', 'bar')
    assert isinstance(ei1.es, Elasticsearch)
    ei2 = ElasticIndex({'hosts': 'localhost'}, 'baz')
    assert isinstance(ei2.es, Elasticsearch)
    return


def test_reset(es):
    ei = ElasticIndex(es, 'dbes-test-reset')
    bqall = {"query": {"match_all": {}}}
    with pytest.raises(NotFoundError):
        es.search(index=ei.index, body=bqall)
    ei.reset()
    es.indices.refresh()
    res = es.search(index=ei.index, body=bqall)
    assert res['hits']['total'] == 0
    return


def test_ingest(es):
    ei = ElasticIndex(es, 'dbes-test-ingest')
    ei.reset()
    doc1 = {"_id": 1, "fruit": "apple"}
    assert ei.ingest(doc1) == 1
    # check ElasticIndex.criteria
    doc2 = {"_id": 2, "fruit": "banana"}
    ei.criteria = lambda e: e['fruit'] != "banana"
    assert ei.ingest(doc2) == 0
    # check ElasticIndex.mapper
    ei.mapper = lambda e: dict(e, fruit=e['fruit'].upper())
    doc3 = {"_id": 3, "fruit": "cantaloupe"}
    assert ei.ingest(doc3) == 1
    bqall = {"query": {"match_all": {}}}
    es.indices.refresh()
    res = es.search(index=ei.index, body=bqall)
    assert res['hits']['total'] == 2
    src = es.get(index=ei.index, doc_type=ei.doc_type, id=3)['_source']
    assert src['fruit'] == 'CANTALOUPE'
    return


def test_devour(es):
    ei = ElasticIndex(es, 'dbes-test-devour')
    ei.reset()
    docs = [
        {"_id": 1, "fruit": "apple"},
        {"_id": 2, "fruit": "banana"},
        {"_id": 3, "fruit": "cantaloupe"},
    ]
    assert ei.devour(docs) == 3
    es.indices.refresh()
    cnt = int(es.cat.count(ei.index, h='count'))
    assert cnt == 3
    return
