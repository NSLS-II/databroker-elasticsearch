#!/usr/bin/env python3

import os
from setuptools import setup, find_packages

import versioneer


MYDIR = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(MYDIR, 'README.md')) as fp:
    long_description = fp.read()

setup(
    name='databroker-elasticsearch',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    packages=find_packages('src'),
    package_dir={'': 'src'},
    description='Databroker Elasticsearch bridge',
    long_description=long_description,
    long_description_content_type='text/markdown',
    zip_safe=False,
    include_package_data=True,
    classifiers = [
        # List of possible values at
        # https://pypi.python.org/pypi?:action=list_classifiers
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
)
