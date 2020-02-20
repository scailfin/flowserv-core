# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for the API service descriptor view."""

import os

from flowserv.service.api import API

import flowserv.config.api as config
import flowserv.tests.db as db
import flowserv.tests.serialize as serialize
import flowserv.version as version
from flowserv.view.factory import DefaultView


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
    api = API(con=con, basedir=str(tmpdir))
    r = api.server().service_descriptor()
    serialize.validate_service_descriptor(r)
    assert r['name'] == config.DEFAULT_NAME
    assert r['version'] == version.__version__
    assert not r['validToken']
    api.users().register_user(username='alice', password='abc')
    token = api.users().login_user(username='alice', password='abc')['token']
    r = api.server(access_token=token).service_descriptor()
    serialize.validate_service_descriptor(r)
    assert r['name'] == config.DEFAULT_NAME
    assert r['version'] == version.__version__
    assert r['validToken']
    assert r['username'] == 'alice'
    # Test initialization with a different set of labels
    labels = {
        'SERVER': {
            'SERVICE_NAME': 'serviceName',
            'SERVICE_VERSION': 'serviceVersion',
            'UNK': None
        }
    }
    api = API(con=con, basedir=str(tmpdir), view=DefaultView(labels=labels))
    r = api.server().service_descriptor()
    assert r['serviceName'] == config.DEFAULT_NAME
    assert r['serviceVersion'] == version.__version__
    assert 'UNK' not in r
