# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for the API service descriptor view."""

import os

from flowserv.view.factory import DefaultView

import flowserv.config.api as config
import flowserv.tests.serialize as serialize
import flowserv.version as version


def test_default_service_descriptor_view(service):
    """Test the service descriptor serialization."""
    # Ensure that the FLOWSERV_API_NAME variable is not set.
    if config.FLOWSERV_API_NAME in os.environ:
        del os.environ[config.FLOWSERV_API_NAME]
    # -- Test service descriptor without information for logged-in user -------
    with service() as api:
        r = api.server().service_descriptor()
        serialize.validate_service_descriptor(r)
        assert r['name'] == config.DEFAULT_NAME
        assert r['version'] == version.__version__
        assert not r['validToken']
    # -- Test service descriptor with user information ------------------------
    username = 'alice'
    pwd = 'abc'
    with service() as api:
        api.users().register_user(username=username, password=pwd)
        r = api.users().login_user(username=username, password=pwd)
        serialize.validate_user_handle(r, login=True)
        api_key = r['token']
        r = api.server(access_token=api_key).service_descriptor()
        serialize.validate_service_descriptor(r)
        assert r['name'] == config.DEFAULT_NAME
        assert r['version'] == version.__version__
        assert r['validToken']
        assert r['username'] == username


def test_service_descriptor_with_custom_labels_view(service):
    """Test serialization for a service descriptor with a custom set of view
    labels.
    """
    # Ensure that the FLOWSERV_API_NAME variable is not set.
    if config.FLOWSERV_API_NAME in os.environ:
        del os.environ[config.FLOWSERV_API_NAME]
    labels = {
        'SERVER': {
            'SERVICE_NAME': 'serviceName',
            'SERVICE_VERSION': 'serviceVersion',
            'UNK': None
        }
    }
    # -- Test initialization with a different set of labels -------------------
    with service(view=DefaultView(labels=labels)) as api:
        r = api.server().service_descriptor()
        assert r['serviceName'] == config.DEFAULT_NAME
        assert r['serviceVersion'] == version.__version__
        assert 'UNK' not in r
