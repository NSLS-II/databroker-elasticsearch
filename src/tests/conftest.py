#!/usr/bin/env python3

import os
import json

import pytest
from elasticsearch import Elasticsearch

# Suggest matplotlib backend that does not need DISPLAY or
# python running as a framework on OS X.
os.environ.setdefault('MPLBACKEND', 'agg')


@pytest.fixture(scope='module')
def es(hosts=None):
    e = Elasticsearch(hosts)
    yield e
    e.indices.delete('dbes-test-*')
    return


@pytest.fixture(scope='module')
def issrecords():
    with open(tdatafile('iss-sample.json')) as fp:
        rv = json.load(fp)
    return rv


@pytest.fixture(scope='module')
def db(request):
    from databroker.tests.utils import build_sqlite_backed_broker
    bk = build_sqlite_backed_broker(request)
    return bk


def tdatafile(filename):
    "Return absolute path to a file in testdata/ directory."
    thisdir = os.path.dirname(os.path.abspath(__file__))
    rv = os.path.join(thisdir, 'testdata', filename)
    return rv
