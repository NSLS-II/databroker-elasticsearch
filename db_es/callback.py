"""Callback for adding data to elastic search from run engine """

from bluesky.callbacks.core import CallbackBase
from elasticsearch import Elasticsearch
from elasticsearch import helpers as eshelpers


def noconversion(x):
    return x


def toisoformat(epoch):
    """Convert epoch seconds to elasticsearch friendly ISO time.

    When `epoch` is a float return ISO date with millisecond
    precision.  Otherwise return date rounded to seconds.

    Parameters
    ----------
    epoch : float
        The time in seconds since POSIX epoch in 1970.

    Returns
    -------
    isodate : str
        The ISO formatted date and time with second or millisecond precision.
    """
    from datetime import datetime

    epochms = round(epoch, 3)
    dt = datetime.fromtimestamp(epochms)
    tiso = dt.isoformat()
    rv = tiso[:-3] if dt.microsecond else tiso
    assert len(rv) in (19, 23)
    return rv


def normalize_counts(d):
    if not isinstance(d, dict):
        return None
    totalcount = sum(d.values()) or 1.0
    rv = dict((k, v / totalcount) for k, v in d.items())
    return rv


def listofstrings(v):
    rv = None
    if isinstance(v, list) and all(isinstance(w, str) for w in v):
        rv = v
    return rv


DOCMAP = {
    "iss": [
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
    ],
    "xpd": [
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
    ],
}


def iss_filter(x):
    return True


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


def esdocument(docmap, entry):
    rv = {}
    for doc_key, esname, fcnv in docmap:
        if not doc_key in entry:
            continue
        doc_value = entry[doc_key]
        evalue = fcnv(doc_value) if doc_value is not None else None
        if evalue is None:
            continue
        rv[esname] = evalue
    return rv


class ElasticInsert(CallbackBase):
    """Callback for inserting start metadata into an ElasticSearch instance

    Examples
    --------
    Run imports
    >>> from elasticsearch import Elasticsearch
    >>> from db_es.callback import ElasticInsert, noconversion, toisoformat
    
    Assuming that the Elasticsearch instance is running on local host
    >>> es = Elasticsearch(hosts=['127.0.0.1'])
   
    Create a list of tuples which is the `('data_name',
    'elasticsearch_name', converter)` The `data_name` is the name in the
    data and `elasticsearch_name` is the name in elasticsearch. The
    converter is a function which converts the data from the type from the
    experiment to a type that elasticsearch can understand, most times
    `noconversion` will work
    >>> docmap = [('bt_piLast', 'pi', noconversion), ('cycle', 'cycle', int)]

    Create an instance of ElasticInsert
    >>> ei = ElasticInsert(es, esindex='xpd', docmap=docmap, beamline='xpd')
    
    Optionally an additional criteria function can be given. When provided
    the function is run on all the incoming data. If the function returns
    True the data will be copied to Elasticsearch, otherwise it will not.
    This can be used to prevent propritary data from being exposed.
    >>> ei = ElasticInsert(es, esindex='xpd', docmap=docmap, beamline='xpd',
    >>>                    criteria=lambda x: 'CJ' in x['bt_piLast'])
    
    Once the callback has been setup it can be subscribed to the RunEngine so
    that all subsequent data is added to elasticsearch
    >>> RE.subscribe(ei)

    Or data from the databroker can be sent through, for initializing
    elasticsearch
    >>> for hdr in db():
    ...     for name, doc in hdr.documents():
    ...         ei(name, doc)
    
    """

    def __init__(
        self,
        es: Elasticsearch,
        esindex: str,
        docmap: list,
        beamline: str,
        criteria=lambda x: True,
    ):
        """Init callback

        Parameters
        ----------
        es : Elasticsearch instance
            The Elasticsearch client to push entries to
        esindex : str
            The index to use in Elasticsearch
        docmap : list
            A map between keys in the start document and keys in the ES
            instance, with conversions
        beamline : str
            Name of beamline
        criteria : callable, optional
            Callable which is run on all the start document, if the return is
            truthy the document is sent to ES else, it is not added. Defaults
            to True for all documents
        """
        self.criteria = criteria
        self.beamline = beamline
        self.esindex = esindex
        self.docmap = docmap
        self.es = es

    def start(self, doc):
        if self.criteria(doc):
            # filter the doc
            # transform the docs
            sanitized_docs = esdocument(self.docmap, doc)
            actions = dict(
                _index=self.esindex,
                # XXX: this might not work?
                _id=doc["uid"],
                _type=self.beamline,
                _source=sanitized_docs,
            )
            self.es.indices.delete(index=self.esindex, ignore_unavailable=True)
            self.es.indices.create(index=self.esindex)
            mbody = {
                "properties": {
                    "time": {"type": "date", "format": "epoch_second"},
                    "date": {
                        "type": "date",
                        "format": "strict_date_optional_time",
                    },
                }
            }
            self.es.indices.put_mapping(
                doc_type=self.beamline, index=self.esindex, body=mbody
            )
            # TODO: use regular insert rather than bulk
            eshelpers.bulk(self.es, [actions])
