import os
import pytest
from elasticsearch import Elasticsearch

# Suggest matplotlib backend that does not need DISPLAY or
# python running as a framework on OS X.
os.environ.setdefault('MPLBACKEND', 'agg')


@pytest.fixture(scope='module')
def es(host="127.0.0.1"):
    e = Elasticsearch(hosts=[host])
    ic = e.indices
    ic.create('dbes-test-xpd')
    ic.create('dbes-test-iss')
    ic.create('dbes-test-bad_xpd')
    yield e
    ic.delete('dbes-test-*')
    return
