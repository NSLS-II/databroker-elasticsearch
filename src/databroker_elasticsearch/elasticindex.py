#!/usr/bin/env python3

"""\
Class for convenient access to Elasticsearch index.
"""

from typing import Callable
from elasticsearch import Elasticsearch
from elasticsearch import helpers as eshelpers


class ElasticIndex:
    """Convenience functions for adding documents to Elasticsearch index.

    Parameters
    ----------
    es : Elasticsearch, str, or dict
        The Elasticsearch client to push entries to.
        When `str` or `dict` type, instantiate a new Elasticsearch object
        using this argument.
    index : str
        The index to use in Elasticsearch
    mapper : callable, optional
        A function to transform input document to Elasticsearch entry.
    criteria : callable, optional
        Callable which is run on all the start document, if the return is
        truthy the document is sent to ES else, it is not added. Defaults
        to True for all documents

    Attributes
    ----------
    es : Elasticsearch
        The Elasticsearch client to push entries to.
    index : str
        The name of the Elasticsearch index to be manipulated.
    mapper : callable or None, optional
        An optional function to transform input document to
        Elasticsearch entry.
    criteria : callable, optional
        Callable which is run on the added document.  If the return
        is True the document is sent to the ES and is ignored otherwise.
        The default is True for all documents.
    doc_type : str
        The name of Elasticsearch document type for added entries.
        The default is "run_start".
    doc_properties : dict
        The field names and data types in the Elasticsearch `doc_type`. [1]_

    References
    ----------
    .. [1] https://www.elastic.co/guide/en/elasticsearch/guide/current/mapping.html
    """

    def __init__(
            self,
            es,
            index: str,
            mapper: Callable=None,
            criteria: Callable=None,
    ):
        self.es = (Elasticsearch(es) if isinstance(es, str)
                   else Elasticsearch(**es) if isinstance(es, dict)
                   else es)
        self.index = index
        self.mapper = mapper
        self.criteria = criteria
        self.doc_type = "run_start"
        self.doc_properties = {
            "time": {"type": "date", "format": "epoch_second"},
            "date": {"type": "date",
                     "format": "strict_date_optional_time||epoch_second"},
        }
        self._verified_index = ''
        return


    @classmethod
    def from_config(cls, config):
        """
        Create a new ElasticIndex instance using a configuration dictionary.

        Parameters
        ----------
        config : dict
            The configuration dictionary that describes ElasticIndex.
            It must contain "databroker-elasticsearch" key.

        Returns
        -------
        ElasticIndex
        """
        from databroker_elasticsearch.elasticdocument import ElasticDocument
        cfg = config['databroker-elasticsearch']
        esdoc = ElasticDocument(cfg['docmap'])
        rv = cls(es=cfg['host'], index=cfg['index'], mapper=esdoc)
        return rv


    def _generate(self, docs):
        """Produce transformed Elasticsearch entries that pass the criteria.

        The transformed documents must have an `_id` key which is then used
        for Elasticsearch unique identifier.

        Parameters
        ----------
        docs : iterable
            The sequence of input documents of dictionary type.

        Yield
        -----
        tuple
            The pairs of (_id, _source) for ES entry identifier and body.
        """
        okdocs = (docs if self.criteria is None
                  else filter(self.criteria, docs))
        entries = (okdocs if self.mapper is None
                   else map(self.mapper, okdocs))
        for e in entries:
            doc = e.copy()
            i = doc.pop('_id')
            yield (i, doc)
        pass


    def _ensure_index_exists(self):
        """Create and reset the index if it does not exist yet.
        """
        if self.index == self._verified_index:
            return
        if not self.es.indices.exists(self.index):
            self.reset()
            assert self.es.indices.exists(self.index)
        self._verified_index = self.index
        # check that short-circuit works or fail with infinite recursion.
        self._ensure_index_exists()
        return


    def reset(self):
        """Remove all data from the `index` and set it up anew.

        Set up mappings for the Elasticsearch `doc_type` according to
        `doc_properties`.
        """
        self.es.indices.delete(index=self.index, ignore_unavailable=True)
        self.es.indices.create(index=self.index)
        mbody = {"properties": self.doc_properties}
        self.es.indices.put_mapping(
            doc_type=self.doc_type, index=self.index, body=mbody)
        return


    def qsearch(self, q=None, **kwargs):
        """
        Search this index using Lucene query string syntax.

        Parameters
        ----------
        q : str, optional
            The string search query in Lucene syntax.
        query : dict, optional, keyword-only
            The search definition using the Query DSL.
            The ``query`` and ``body`` arguments  are equivalent and
            only one of them can be specified.
        kwargs : misc, optional
            Extra arguments passed to the `Elasticsearch.search` function.

        Returns
        -------
        dict
            The Elasticsearch response with matching hits.
        """
        kw = dict(q=q, index=self.index)
        clashing_args = set(kw).intersection(kwargs)
        bq = set(('body', 'query'))
        if bq.issubset(kwargs):
            clashing_args.update(bq)
        if clashing_args:
            emsg = ("Conficting keyword arguments: " +
                    ', '.join(clashing_args))
            raise TypeError(emsg)
        if 'query' in kwargs:
            kw['body'] = kwargs.pop('query')
        kw.update(kwargs)
        rv = self.es.search(**kw)
        return rv


    def ingest(self, doc):
        """Convert and insert one document to ES if it passes `criteria`.

        When `index` does not exist, call `reset` to set it up.

        Returns
        -------
        int
            Number of added documents, 0 or 1.
        """
        cnt = 0
        self._ensure_index_exists()
        for i, body in self._generate([doc]):
            self.es.index(index=self.index, doc_type=self.doc_type,
                          id=i, body=body)
            cnt += 1
        return cnt


    def devour(self, docs):
        """Convert and add many documents to ES if they pass `criteria`.

        When `index` does not exist, call `reset` to set it up.

        Returns
        -------
        int
            Number of added documents.
        """
        self._ensure_index_exists()
        a = {"_index": self.index, "_type": self.doc_type}
        actions = ((a, a.update(_id=i, _source=src))[0]
                   for i, src in self._generate(docs))
        res = eshelpers.bulk(self.es, actions)
        return res[0]

# end of class
