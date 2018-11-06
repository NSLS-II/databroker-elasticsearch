import pytest

from databroker_elasticsearch.callback import ElasticInsert, noconversion

# TODO: fill these in with prototype data!
xpd_doc = {"bl": "xpd", "uid": "hi"}
xpd_bad_doc = {"bt_piLast": "not Simon", "bl": "xpd", "uid": "hi1"}
iss_doc = {"bl": "iss", "uid": "hi2"}


xpd_pis = (
    "0713_test",
    "Abeykoon",
    "Antonaropoulos",
    "Assefa",
    "Banerjee",
    "Benjiamin",
    "Billinge",
    "Bordet",
    "Bozin",
    "Demo",
    "Dooryhee",
    "Frandsen",
    "Ghose",
    "Hanson",
    "Milinda and Runze",
    "Milinda",
    "Pinero",
    "Robinson",
    "Sanjit",
    "Shi",
    "Test",
    "Yang",
    "billinge",
    "simulation",
    "test",
    "testPI",
    "testPI_2",
    "testTake2",
    "xpdAcq_realase",
)


def xpd_filter(x):
    if "bt_piLast" not in x:
        return True
    elif x["bt_piLast"] in xpd_pis:
        return True
    else:
        return False


@pytest.mark.parametrize(
    "idx, dm, bl, f, doc",
    zip(
        ["dbes-test-xpd", "dbes-test-iss"],
        [
            [("uid", "uid", noconversion), ("bl", "bl", noconversion)],
            [("uid", "uid", noconversion), ("bl", "bl", noconversion)],
        ],
        ["xpd", "iss"],
        [xpd_filter, lambda x: True],
        [xpd_doc, iss_doc],
    ),
)
def test_callback(es, idx, dm, bl, f, doc):
    cb = ElasticInsert(es=es, esindex=idx, docmap=dm, beamline=bl, criteria=f)
    cb("start", doc)
    es.indices.flush()
    res = es.search(idx, body={"query": {"match_all": {}}})
    assert res["hits"]["total"] == 1


def test_no_op_callback(es):
    cb = ElasticInsert(
        es=es,
        esindex="dbes-test-bad_xpd",
        docmap=[("uid", "uid", noconversion), ("bl", "bl", noconversion)],
        beamline="xpd",
        criteria=xpd_filter,
    )
    cb("start", xpd_bad_doc)
    es.indices.flush()
    res = es.search("dbes-test-bad_xpd", body={"query": {"match_all": {}}})
    assert res["hits"]["total"] == 0
