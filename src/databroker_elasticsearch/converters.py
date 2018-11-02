#!/usr/bin/env python3

"""\
Functions for converting values that are exported to Elasticsearch.
"""

def noconversion(x):
    "Return the argument as is."
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
    """Normalize numeric values in a dictionary to a total of 1.

    For obtaining normalized stoichiometry in a chemical formula.

    Parameters
    ----------
    d : dict or collections.OrderedDict
        The dictionary to be normalized.

    Returns
    -------
        A copy of `d` with values summing to a total of 1.
        Return ``None`` when `d` is of invalid type.
    """
    if not isinstance(d, dict):
        return None
    rv = d.copy()
    totalcount = sum(d.values()) or 1.0
    rv.update((k, v / totalcount) for k, v in d.items())
    return rv


def listofstrings(v):
    """Return argument if it is a list of strings or None if not.
    """
    rv = None
    if isinstance(v, list) and all(isinstance(w, str) for w in v):
        rv = v
    return rv
