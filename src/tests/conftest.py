import pytest
from elasticsearch import Elasticsearch


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
