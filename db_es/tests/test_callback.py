import pytest

from db_es.callback import ElasticInsert, DOCMAP, xpd_filter, iss_filter

# TODO: fill these in with prototype data!
xpd_doc = {"bl": "xpd", 'uid': 'hi'}
xpd_bad_doc = {"bt_piLast": "not Simon", "bl": "xpd", 'uid': 'hi1'}
iss_doc = {"bl": "iss", 'uid': 'hi2'}


@pytest.mark.parametrize(
    "idx, dm, bl, f, doc",
    zip(
        ["xpd", "iss"],
        [DOCMAP["xpd"],
         DOCMAP["iss"]],
        ["xpd", "iss"],
        [xpd_filter, iss_filter],
        [xpd_doc, iss_doc],
    ),
)
def test_callback(es, idx, dm, bl, f, doc):
    cb = ElasticInsert(es=es, esindex=idx, docmap=dm, beamline=bl, criteria=f)
    cb("start", doc)
    # Search ES find thing
    res = es.search(idx, body={"query": {"match": {"test": bl}}})
    assert len(res["hits"]["hits"]) == 1


def test_no_op_callback(es):
    cb = ElasticInsert(
        es=es,
        esindex="xpd",
        docmap=DOCMAP["xpd"],
        beamline="xpd",
        criteria=xpd_filter,
    )
    cb("start", xpd_bad_doc)
    # Search ES find thing
    res = es.search("xpd", body={"query": {"match": {"test": "world"}}})
    assert len(res["hits"]["hits"]) == 0
