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


def callback_from_name(name):
    """Construct ElasticCallback from YAML configuration in standard paths.

    Parameters
    ----------
    name : str
        The base name of YAML configuration file to be found and loaded.
        For example, the name "xyz-es" would be looked up as

        * ``~/.config/databroker/xyz-es.yml``
        * ``{python}/../etc/databroker/xyz-es.yml``
        * ``/etc/databroker/xyz-es.yml``

    Returns
    -------
    ElasticCallback
        The new instance of ElasticCallback.
    """
    from databroker._core import lookup_config
    cfg = lookup_config(name)
    rv = callback_from_config(cfg)
    return rv


def load_callback(filename):
    """Construct ElasticCallback from a YAML configuration file.

    Parameters
    ----------
    filename : str
        The path to YAML file with ElasticCallback configuration.

    Returns
    -------
    ElasticCallback
        The new instance of ElasticCallback.
    """
    import yaml
    with open(filename) as fp:
        cfg = yaml.full_load(fp)
    rv = callback_from_config(cfg)
    return rv


def load_elasticindex(filename):
    """Construct ElasticIndex from a YAML configuration file.

    Parameters
    ----------
    filename : str
        The path to YAML file with ElasticIndex configuration.

    Returns
    -------
    ElasticIndex
        The new instance of ElasticIndex.
    """
    import yaml
    from databroker_elasticsearch.elasticindex import ElasticIndex
    with open(filename) as fp:
        cfg = yaml.full_load(fp)
    rv = ElasticIndex.from_config(cfg)
    return rv
