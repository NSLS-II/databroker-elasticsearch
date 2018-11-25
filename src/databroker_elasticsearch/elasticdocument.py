#!/usr/bin/env python3

"""\
Class for translating dictionary document to Elasticsearch entry.
"""

from databroker_elasticsearch.converters import getconverter

class ElasticDocument:

    def __init__(self, docmap):
        self.docmap = []
        for specs in docmap:
            ii = iter(specs)
            dname = next(ii)
            ename = next(ii, dname)
            cnv = next(ii, 'noconversion')
            fcnv = cnv if callable(cnv) else getconverter(cnv)
            self.docmap.append((dname, ename, fcnv))
        return


    def __call__(self, entry):
        rv = {}
        for dname, ename, fcnv in self.docmap:
            if not dname in entry:
                continue
            dvalue = entry[dname]
            evalue = fcnv(dvalue) if dvalue is not None else None
            if evalue is None:
                continue
            rv[ename] = evalue
        return rv

# end of class
