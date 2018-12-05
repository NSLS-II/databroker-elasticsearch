#!/usr/bin/env python3

"""\
Definition of version metadata.
"""

__all__ = ['__date__', '__git_commit__', '__timestamp__', '__version__']

import time

from ._version import get_versions


v = get_versions()
__version__ = v['version']
__date__ = v['date']
__git_commit__ = v['full-revisionid']

# convert date into epoch seconds when available
__timestamp__ = None
if __date__ is not None:
    __timestamp__ = time.mktime(
        time.strptime(__date__, '%Y-%m-%dT%H:%M:%S%z'))
    __timestamp__ = int(__timestamp__)

# clean up
del v
