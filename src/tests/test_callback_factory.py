#!/usr/bin/env python3

"""\
Test the callback_from_config() factory method.
"""

import yaml

from databroker_elasticsearch import callback_from_config
from conftest import tdatafile

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
