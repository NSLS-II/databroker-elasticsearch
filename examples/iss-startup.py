"""This is an example for inserting documents into Elasticsearch as they come
off the RunEngine.  This is intended as a startup script for the IPython
`collection` profile and should run after RunEngine has been initialized.

See iss-esconfig.yml for Elasticsearch IP address and for conversion
specification between the input databroker document and Elasticsearch entry.
"""

import os.path
import databroker_elasticsearch

# muzzle pyflakes so they don't complain about undefined `RE`
RE = locals().get('RE')

# resolve path to configuration file iss-esconfig.yml
_mydir = os.path.dirname(__file__)
_esconfig = os.path.join(_mydir, 'iss-esconfig.yml')

escb = databroker_elasticsearch.load_callback(_esconfig)

# activate the callback
RE.subscribe(escb)
