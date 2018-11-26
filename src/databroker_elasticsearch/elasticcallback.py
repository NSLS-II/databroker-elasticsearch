#!/usr/bin/env python3

"""\
Callback for adding data to elastic search from run engine.
"""

from bluesky.callbacks.core import CallbackBase
from databroker_elasticsearch.elasticindex import ElasticIndex


class ElasticCallback(CallbackBase):
    "FIXME" + 0 * \
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

    def __init__(self, esindex: ElasticIndex):
        "FIXME" + 0 * \
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
        self.esindex = esindex
        return


    def start(self, doc):
        # FIXME should we cache and ingest in stop() to avoid canceled runs?
        self.esindex.ingest(doc)
        return


    def rebuild(self, db):
        startdocs = (hdr.start for hdr in db())
        self.esindex.reset()
        cnt = self.esindex.devour(startdocs)
        return cnt
