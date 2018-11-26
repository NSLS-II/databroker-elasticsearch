#!/usr/bin/env python3

"""\
Functions for converting values that are exported to Elasticsearch.
"""

# handle registration of converter functions ---------------------------------

def register_converter(f, name=None):
    "Decorator to mark up some function as a converter."
    nm = f.__name__ if name is None else name
    _converters[nm] = f
    return f

_converters = {}


def getconverter(name):
    "Return converter function of the specified name."
    return _converters[name]

# define and register converter functions ------------------------------------

register_converter(int)
register_converter(float)
register_converter(str)


@register_converter
def noconversion(x):
    "Return the argument as is."
    return x


@register_converter
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


@register_converter
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


@register_converter
def listofstrings(v):
    """Return argument if it is a list of strings or None if not.
    """
    rv = None
    if isinstance(v, list) and all(isinstance(w, str) for w in v):
        rv = v
    return rv
