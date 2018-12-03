#!/usr/bin/env python3

"""\
Test the ElasticDocument translator class.
"""

from databroker_elasticsearch.elasticdocument import ElasticDocument
from databroker_elasticsearch import converters


def test___init__():
    esdoc = ElasticDocument([['_id'], ['name', 'uname', str.upper]])
    assert len(esdoc.docmap) == 2
    assert all(3 == len(tp) for tp in esdoc.docmap)
    assert ('_id', '_id', converters.noconversion) == esdoc.docmap[0]
    assert ('name', 'uname', str.upper) == esdoc.docmap[1]
    return


def test___call__():
    esdoc = ElasticDocument([['_id'], ['names', 'names', 'listofstrings']])
    src = {'_id': 1, 'names': ['Alice', 'Bob']}
    assert src == esdoc(src)
    src['names'] = ['Alice', 22]
    assert {'_id': 1} == esdoc(src)
    src.pop('names')
    assert {'_id': 1} == esdoc(src)
    return
