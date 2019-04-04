#!/usr/bin/env python3

"""\
Retrieve databroker Headers correspoding to an Elasticsearch query.
"""

from databroker._core import Results


class BrokerSearch:
    """
    Callable to retrieve databroker `Header` objects from ES query.

    Attributes
    ----------
    db : databroker.Broker
        The databroker instance for looking up `Header` objects.
    esindex : ElasticIndex
        The ElasticIndex instance to use for ES queries.
    """

    def __init__(self, db, esindex):
        self.db = db
        self.esindex = esindex
        return


    def __call__(self, q=None, **kwargs):
        """
        Execute search using Lucene query string syntax.

        Parameters
        ----------
        q : str, optional
            The string search query in Lucene syntax.
        query : dict, optional, keyword-only
            The search definition using the Query DSL.
        kwargs : misc, optional
            Extra arguments passed to the `Elasticsearch.search` function.

        Returns
        -------
        databroker.Results
            Iterable object encapsulating the matching databroker Headers.
        """
        from elasticsearch.helpers import scan
        es = self.esindex.es
        index = self.esindex.index
        gscan = scan(es, q=q, index=index, _source=['uid'], **kwargs)
        # FIXME: we need a fetchstart to work around bug in databroker.
        # Remove the fetchstart mapping when fixed.
        fetchstart = self.db.hs.mds.run_start_given_uid
        gstartstop = ((fetchstart(e['_source']['uid']), None) for e in gscan)
        rv = Results(gstartstop, self.db, data_key=None)
        return rv
