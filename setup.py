#!/usr/bin/env python3

from setuptools import setup, find_packages

import versioneer


setup(
    name='databroker-elasticsearch',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    packages=find_packages('src'),
    package_dir={'': 'src'},
    description='Databroker ElasticSearch bridge',
    zip_safe=False,
    include_package_data=True,
)
