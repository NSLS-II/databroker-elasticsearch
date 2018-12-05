#!/usr/bin/env python3

"""\
Test version metadata extraction.
"""

from databroker_elasticsearch import version


def test___git_commit__():
    assert isinstance(version.__git_commit__, str)
    assert len(version.__git_commit__) >= 40
    return


def test___timestamp__():
    assert version.__timestamp__ > 1543986000
    return
