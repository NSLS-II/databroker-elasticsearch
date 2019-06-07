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
    zip_safe=False,
    include_package_data=True,
    description='Databroker Elasticsearch bridge',
    long_description=long_description,
    long_description_content_type='text/markdown',
    license='BSD (3-clause)',
    url='https://github.com/NSLS-II/databroker-elasticsearch',
    classifiers = [
        # List of possible values at
        # https://pypi.python.org/pypi?:action=list_classifiers
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
)
