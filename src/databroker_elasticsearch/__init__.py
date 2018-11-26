#!/usr/bin/env python3

"""\
Utilities for exporting databroker documents to Elasticsearch.
"""


from ._version import get_versions
__version__ = get_versions()['version']
del get_versions


def callback_from_config(config):
    """Construct ElasticCallback instance from a configuration dictionary.

    Parameters
    ----------
    config : dict
        The dictionary which contains "databroker-elasticsearch" key.

    Returns
    -------
    ElasticCallback
        The new instance of ElasticCallback.
    """
    from databroker_elasticsearch.elasticcallback import ElasticCallback
    rv = ElasticCallback.from_config(config)
    return rv
