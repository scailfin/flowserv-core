# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for the API service descriptor view."""

import flowserv.config.api as config
import flowserv.tests.serialize as serialize
import flowserv.version as version
from flowserv.view.factory import DefaultView


def test_default_service_descriptor(api_factory):
    """Test the service descriptor serialization."""
    api = api_factory()
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


def test_service_descriptor_with_custom_labels(api_factory):
    """Test serialization for a service descriptor with a custom set of view
    labels.
    """
    # Test initialization with a different set of labels
    labels = {
        'SERVER': {
            'SERVICE_NAME': 'serviceName',
            'SERVICE_VERSION': 'serviceVersion',
            'UNK': None
        }
    }
    api = api_factory(view=DefaultView(labels=labels))
    r = api.server().service_descriptor()
    assert r['serviceName'] == config.DEFAULT_NAME
    assert r['serviceVersion'] == version.__version__
    assert 'UNK' not in r
