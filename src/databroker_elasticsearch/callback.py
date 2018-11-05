"""Callback for adding data to elastic search from run engine """

from bluesky.callbacks.core import CallbackBase
from elasticsearch import Elasticsearch
from elasticsearch import helpers as eshelpers


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
    >>> from databroker_elasticsearch.callback import ElasticInsert
    >>> from databroker_elasticsearch.callback import noconversion, toisoformat

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
        return


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
        return
