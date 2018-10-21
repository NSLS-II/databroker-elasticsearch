import pytest
from elasticsearch import Elasticsearch


@pytest.fixture(scope='module')
def es(host="127.0.0.1"):
    e = Elasticsearch(hosts=[host])
    ic = e.IndicesClient()
    ic.create('xpd')
    ic.create('iss')
    return e
