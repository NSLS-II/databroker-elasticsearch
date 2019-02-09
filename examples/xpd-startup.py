"""Example startup file for inserting databroker documents into Elasticsearch.
The conversion specification is entirely defined in the script.
We also use document filtering to insert only allowed entries.
"""

import re
from databroker_elasticsearch.elasticcallback import ElasticCallback
from databroker_elasticsearch.elasticindex import ElasticIndex
from databroker_elasticsearch.elasticdocument import ElasticDocument

# hostname of Elasticsearch server
eshost = "localhost"
# Elasticsearch index where to export the documents
esindex = "xpd"

# At XPD we only export measurements from specific PI-s and ignore all others.
# We define `xpd_ok` function that returns True for allowed "start" documents.

_rxpi = re.compile(r"""
    # match empty string for undefined PI
    ^$
    # allowed PI names
    |0713_test
    |Abeykoon
    |Antonaropoulos
    |Assefa
    |Banerjee
    |Benjiamin
    |Billinge
    |Bordet
    |Bozin
    |Demo
    |Dooryhee
    |Frandsen
    |Ghose
    |Hanson
    |Milinda and Runze
    |Milinda
    |Pinero
    |Robinson
    |Sanjit
    |Shi
    |Test
    |Yang
    |billinge
    |simulation
    |test
    |testPI
    |testPI_2
    |testTake2
    |xpdAcq_realase
    """, re.VERBOSE)

def xpd_ok(startdoc):
    """Return value that tests True if document should be exported to ES.

    Return RE match document for the white-listed PI.
    Return None when PI is not allowed.
    """
    btpi = startdoc.get("bt_piLast", "")
    return _rxpi.search(btpi)


# Create esdoc callable object for transforming "start" documents to ES entry.
# Converter may be a string name of registered converter - see
# `pydoc databroker_elasticsearch.converters` for allowed names.

# define our own conversion utility
def toyear(tstamp):
    from time import localtime
    return localtime(tstamp).tm_year

esdoc = ElasticDocument(docmap=[
    # docname  [esname=docname]  [converter=noconversion]
    ["uid", "_id", str], # the mapping must produce ES name "_id"
    ["bt_experimenters", "experimenters", "listofstrings"],
    ["bt_piLast", "pi", str],
    ["bt_safN", "saf", str],
    ["bt_wavelength", "wavelength", float],
    ["composition_string", "formula"], # converter defaults to "noconversion"
    ["dark_frame", "dark_frame", bool],
    ["group"], # esname defaults to docname
    ["lead_experimenter", "pi"],
    ["notes", "comment"],
    ["num_points", "num_points"],
    ["plan_name"],
    ["sample_composition", "composition", "normalize_counts"],
    ["scan_id", "scan_id"],
    ["sp_computed_exposure", "sp_computed_exposure", float],
    ["sp_num_frames", "sp_num_frames", int],
    ["sp_plan_name"],
    ["sp_time_per_frame", "sp_time_per_frame", float],
    ["sp_type"],
    ["time"],
    ["time", "date", "toisoformat"],
    ["uid", "uid"],
    ["time", "year", toyear],
])

# Create esindex interface object to Elasticsearch
esindex = ElasticIndex(eshost, index=esindex, mapper=esdoc, criteria=xpd_ok)

# Create and activate callback object for inserting documents
escb = ElasticCallback(esindex)

# muzzle pyflakes so they don't complain about undefined `RE`
RE = locals().get('RE')

# activate the callback
RE.subscribe(escb)
