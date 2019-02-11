#!/usr/bin/env python3

"""\
Class for translating dictionary document to Elasticsearch entry.
"""

from databroker_elasticsearch.converters import getconverter


class ElasticDocument:
    """Transform dictionary document according to the `docmap`.

    Convert selected items in the input dictionary and return
    them in a new output dictionary under the same or different
    keys.  Skip items that have a value of ``None`` or which
    convert to ``None``.

    Parameters
    ----------
    docmap : list of tuples or list of lists
        The specification of dictionary conversion provided as
        a list of tuples ``(keyin, keyout, converter)``.
        Each tuple sets ``docout[keyout] = converter(docin[keyin])``
        with missing keys in docin silently ignored.
        The tuples may be abbreviated to one or two entries which
        will imply ``keyout = keyin`` and ``converter = noconversion``.
        The `converter` can be either a callable or a string name
        registered with the `getconverter` function.

    Attributes
    ----------
    docmap : list of tuples
        The finalized specification for dictionary conversion.
        This equals the `docmap` argument of class initialization
        except that abbreviated `docmap` entries are expanded and
        string-type converters replaced with corresponding function.
    """

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
        """
        Convert dictionary document according to `docmap` specification.

        Parameters
        ----------
        entry : dict
            The input dictionary document.

        Returns
        -------
        dict
            The transformed document in a new dictionary object.
        """
        rv = {}
        for dname, ename, fcnv in self.docmap:
            if dname not in entry:
                continue
            dvalue = entry[dname]
            evalue = fcnv(dvalue) if dvalue is not None else None
            if evalue is None:
                continue
            rv[ename] = evalue
        return rv

# end of class
