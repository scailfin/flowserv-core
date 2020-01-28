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
import flowserv.tests.db as db
import flowserv.tests.serialize as serialize
import flowserv.version as version


"""API environment variables that control the base url."""
API_VARS = [
    config.FLOWSERV_API_NAME,
    config.FLOWSERV_API_HOST,
    config.FLOWSERV_API_PORT,
    config.FLOWSERV_API_PROTOCOL,
    config.FLOWSERV_API_PATH
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
    serialize.validate_service_descriptor(r)
    assert r['name'] == config.DEFAULT_NAME
    assert r['version'] == version.__version__
    for link in r['links']:
        assert link['href'].startswith('http://localhost/')
    # Test initialization of the UrlFactory
    api = API(con=con, urls=UrlFactory(base_url='http://www.flowserv.org////'))
    r = api.service_descriptor()
    for link in r['links']:
        ref = link['href']
        if ref == 'http://www.flowserv.org':
            continue
        assert ref.startswith('http://www.flowserv.org/')
        assert not ref.startswith('http://www.flowserv.org//')
