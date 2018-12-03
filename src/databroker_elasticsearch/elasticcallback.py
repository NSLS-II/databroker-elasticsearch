#!/usr/bin/env python3

"""\
Callback for adding data to elastic search from run engine.
"""

from bluesky.callbacks.core import CallbackBase
from databroker_elasticsearch.elasticindex import ElasticIndex


class ElasticCallback(CallbackBase):
    """
    Callback for inserting start metadata into an `Elasticsearch` instance.

    This wraps `ElasticIndex` adapter for adding translated documents to
    a given Elasticsearch index.

    Attributes
    ----------
    esindex : ElasticIndex
        The ElasticIndex object for converting documents and inserting
        them to Elasticsearch.
    """


    def __init__(self, esindex: ElasticIndex):
        self.esindex = esindex
        return


    @classmethod
    def from_config(cls, config):
        """
        Create a new ElasticCallback instance using a configuration dictionary.

        Parameters
        ----------
        config : dict
            The configuration dictionary that describes ElasticIndex.
            It must contain "databroker-elasticsearch" key.

        Returns
        -------
        ElasticCallback
        """
        from databroker_elasticsearch.elasticindex import ElasticIndex
        esindex = ElasticIndex.from_config(config)
        rv = cls(esindex)
        return rv


    def rebuild(self, db):
        """Purge ES index and re-export all start documents from databroker.

        Parameters
        ----------
        db : databroker.Broker
            The Broker instance that has all prior "start" documents.
        """
        startdocs = (hdr.start for hdr in db())
        self.esindex.reset()
        cnt = self.esindex.devour(startdocs)
        return cnt

    # override CallbackBase function

    def start(self, doc):
        """Handle "start" document in bluesky run engine callback.
        """
        # FIXME should we cache and ingest in stop() to avoid canceled runs?
        self.esindex.ingest(doc)
        return
