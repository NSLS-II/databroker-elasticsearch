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


def test_qsearch(es, issrecords):
    ei = ElasticIndex(es, 'dbes-test-qsearch')
    ei.reset()
    ei.devour(issrecords)
    es.indices.refresh()
    cntall = len(issrecords)
    res = ei.qsearch()
    assert res['hits']['total'] == cntall
    res = ei.qsearch('*')
    assert res['hits']['total'] == cntall
    res = ei.qsearch(body={'query': {'match_all': {}}})
    assert res['hits']['total'] == cntall
    res = ei.qsearch(query={'query': {'match_all': {}}})
    assert res['hits']['total'] == cntall
    res = ei.qsearch('scan_id:>42500')
    assert res['hits']['total'] == 2
    res = ei.qsearch('uid:73f4e8fa')
    assert res['hits']['total'] == 1
    src = res['hits']['hits'][0]['_source']
    assert src["name"] == "Ti-ZnO NW III cycle 11"
    with pytest.raises(TypeError):
        res = ei.qsearch('*', index='dummy')
    # body and query arguments are exclusive
    with pytest.raises(TypeError):
        res = ei.qsearch(body={}, query={})
    return


def test_date_tz(es, issrecords):
    from databroker_elasticsearch.elasticdocument import ElasticDocument
    docmap = [['uid', '_id'], ['time'], ['time', 'date', 'toisoformat']]
    esdoc = ElasticDocument(docmap)
    ei = ElasticIndex(es, 'dbes-test-date', mapper=esdoc)
    ei.devour(issrecords)
    es.indices.refresh()
    hits = lambda q: ei.qsearch(q)['hits']['total']
    assert 0 == hits('date:[* TO 2018-02-10T19:36:00-05:00]')
    assert 1 == hits('date:[* TO 2018-02-10T19:37:00-05:00]')
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
