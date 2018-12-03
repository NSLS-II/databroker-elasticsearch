import os
import pytest
from elasticsearch import Elasticsearch

# Suggest matplotlib backend that does not need DISPLAY or
# python running as a framework on OS X.
os.environ.setdefault('MPLBACKEND', 'agg')


@pytest.fixture(scope='module')
def es(hosts=None):
    e = Elasticsearch(hosts)
    ic = e.indices
    ic.create('dbes-test-xpd')
    ic.create('dbes-test-iss')
    ic.create('dbes-test-bad_xpd')
    yield e
    ic.delete('dbes-test-*')
    return


def tdatafile(filename):
    "Return absolute path to a file in testdata/ directory."
    thisdir = os.path.dirname(os.path.abspath(__file__))
    rv = os.path.join(thisdir, 'testdata', filename)
    return rv
