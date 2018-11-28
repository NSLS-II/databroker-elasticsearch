#!/usr/bin/env python3

"""\
Utilities for exporting databroker documents to Elasticsearch.
"""

import os.path


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


def callback_from_name(name):
    """Construct ElasticCallback instance from a YAML configuration file.

    Parameters
    ----------
    name : str
        The base name of YAML configuration file to be found and loaded.
        For example, the name "xyz-es" would be looked up as

        * ``~/.config/databroker/xyz-es.yml``
        * ``{python}/../etc/databroker/xyz-es.yml``
        * ``/etc/databroker/xyz-es.yml``

        If `name` contains a path separator, e.g., "./es.yml" it is used
        as configuration filename and the lookup above is skipped.

    Returns
    -------
    ElasticCallback
        The new instance of ElasticCallback created from configuration.
    """
    import yaml
    from databroker._core import lookup_config
    if '/' in name or os.path.sep in name:
        with open(name) as fp:
            cfg = yaml.load(fp)
    else:
        cfg = lookup_config(name)
    rv = callback_from_config(cfg)
    return rv
