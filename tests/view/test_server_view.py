# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for the API service descriptor view."""

import os

from flowserv.service.server import Service
from flowserv.view.route import UrlFactory

import flowserv.config.api as config
import flowserv.view.hateoas as hateoas
import flowserv.view.labels as labels
import flowserv.tests.serialize as serialize
import flowserv.version as version
import flowserv.core.util as util


RELS = [
    hateoas.SELF,
    hateoas.LOGIN,
    hateoas.LOGOUT,
    hateoas.REGISTER,
    hateoas.BENCHMARKS,
    hateoas.SUBMISSIONS
]


def test_descriptor():
    """Test the service descriptor serialization."""
    # Clear environment variable if set
    if config.ROB_API_NAME in os.environ:
        del os.environ[config.ROB_API_NAME]
    r = Service().service_descriptor()
    util.validate_doc(
        doc=r,
        mandatory_labels=[
            labels.NAME,
            labels.LINKS,
            labels.VERSION,
            labels.VALID_TOKEN
        ]
    )
    serialize.validate_links(r, RELS)
    assert r[labels.NAME] == config.DEFAULT_NAME
    assert r[labels.VERSION] == version.__version__
    for link in r[labels.LINKS]:
        assert link[labels.REF].startswith('http://localhost')
    # Test initialization of the UrlFactory
    urls = UrlFactory(base_url='http://www.rob.org////')
    r = Service(urls=urls).service_descriptor()
    for link in r[labels.LINKS]:
        ref = link[labels.REF]
        if ref == 'http://www.rob.org':
            continue
        assert ref.startswith('http://www.rob.org/')
        assert not ref.startswith('http://www.rob.org//')
