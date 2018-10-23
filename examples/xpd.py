"""This is an example for inserting documents into Elasticsearch in bulk from
a databroker.

Note that the host IP address may need to be changed"""
from elasticsearch import Elasticsearch
from db_es.callback import ElasticInsert, noconversion, toisoformat, \
        listofstrings, normalize_counts

docmap = [
        # docname  esname  converter
        ("bt_experimenters", "experimenters", listofstrings),
        ("bt_piLast", "pi", noconversion),
        ("bt_safN", "saf", str),
        ("bt_wavelength", "wavelength", float),
        ("composition_string", "formula", noconversion),
        ("dark_frame", "dark_frame", bool),
        ("group", "group", noconversion),
        ("lead_experimenter", "pi", noconversion),
        ("notes", "comment", noconversion),
        ("num_points", "num_points", noconversion),
        ("plan_name", "plan_name", noconversion),
        ("sample_composition", "composition", normalize_counts),
        ("scan_id", "scan_id", noconversion),
        ("sp_computed_exposure", "sp_computed_exposure", float),
        ("sp_num_frames", "sp_num_frames", int),
        ("sp_plan_name", "sp_plan_name", noconversion),
        ("sp_time_per_frame", "sp_time_per_frame", float),
        ("sp_type", "sp_type", noconversion),
        ("time", "time", noconversion),
        ("time", "date", toisoformat),
        ("uid", "uid", noconversion),
        ("time", "year", lambda t: int(toisoformat(t)[:4])),
    ]
es = Elasticsearch(hosts=['127.0.0.1'])

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

ei = ElasticInsert(es, esindex='xpd', docmap=docmap, beamline='xpd',
                   criteria=xpd_filter)

from databroker import Broker

db = Broker.named('xpd')

for hdr in db():
    ei('start', hdr.start)
