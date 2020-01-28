# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for the API service descriptor view."""

import os

from flowserv.service.server import Service
from flowserv.service.api import API
from flowserv.view.route import UrlFactory

import flowserv.config.api as config
import flowserv.core.util as util
import flowserv.tests.db as db
import flowserv.tests.serialize as serialize
import flowserv.view.hateoas as hateoas
import flowserv.view.labels as labels
import flowserv.version as version


API_VARS = [
    config.FLOWSERV_API_NAME,
    config.FLOWSERV_API_HOST,
    config.FLOWSERV_API_PORT,
    config.FLOWSERV_API_PROTOCOL,
    config.FLOWSERV_API_PATH
]


RELS = [
    hateoas.SELF,
    hateoas.LOGIN,
    hateoas.LOGOUT,
    hateoas.REGISTER,
    hateoas.WORKFLOWS,
    hateoas.GROUPS
]


def test_service_descriptor(tmpdir):
    """Test the service descriptor serialization."""
    # Clear environment variables if set
    for var in API_VARS:
        if var in os.environ:
            del os.environ[var]
    os.environ[config.FLOWSERV_API_PORT] = '80'
    con = db.init_db(str(tmpdir)).connect()
    api = API(con=con)
    r = api.service_descriptor()
    util.validate_doc(
        doc=r,
        mandatory=[
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
        assert link[labels.REF].startswith('http://localhost/')
    # Test initialization of the UrlFactory
    api = API(con=con, urls=UrlFactory(base_url='http://www.flowserv.org////'))
    r = api.service_descriptor()
    for link in r[labels.LINKS]:
        ref = link[labels.REF]
        if ref == 'http://www.flowserv.org':
            continue
        assert ref.startswith('http://www.flowserv.org/')
        assert not ref.startswith('http://www.flowserv.org//')
