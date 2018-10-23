"""This is an example for inserting documents into Elasticsearch as they come
off the RunEngine. This is intended to be put into the collection ipython
profile after the RunEngine has been initialized.

Note that the host IP address may need to be changed"""
from elasticsearch import Elasticsearch
from db_es.callback import ElasticInsert, noconversion, toisoformat

docmap = [
        # docname  esname  converter
        ("comment", "comment", noconversion),
        ("cycle", "cycle", int),
        ("detectors", "detectors", noconversion),
        ("e0", "e0", noconversion),
        ("edge", "edge", noconversion),
        ("element", "element", noconversion),
        ("experiment", "experiment", noconversion),
        ("group", "group", noconversion),
        ("name", "name", noconversion),
        ("num_points", "num_points", noconversion),
        ("plan_name", "plan_name", noconversion),
        ("PI", "pi", noconversion),
        ("PROPOSAL", "proposal", noconversion),
        ("SAF", "saf", noconversion),
        ("scan_id", "scan_id", noconversion),
        ("time", "time", noconversion),
        ("trajectory_name", "trajectory_name", noconversion),
        ("uid", "uid", noconversion),
        ("year", "year", int),
        ("time", "date", toisoformat),
    ]
es = Elasticsearch(hosts=['127.0.0.1'])

ei = ElasticInsert(es, esindex='iss', docmap=docmap, beamline='iss')

RE.subscribe(ei)
