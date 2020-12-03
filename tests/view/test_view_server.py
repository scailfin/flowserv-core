# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the service descriptor view."""

import pytest

from flowserv.view.server import ServiceSerializer
from flowserv.view.validate import validator

import flowserv.view.server as labels


@pytest.mark.parametrize('username', ['alice', None])
def test_service_descriptor_serialization(username):
    """Validate the serialization of the service descriptor."""
    schema = validator('ServiceDescriptor')
    serializer = ServiceSerializer()
    doc = serializer.service_descriptor(
        name='Test',
        version='0.0.0',
        routes={'home': '/'},
        username=username
    )
    schema.validate(doc)
    assert doc[labels.SERVICE_NAME] == 'Test'
    assert doc[labels.SERVICE_VERSION] == '0.0.0'
    assert len(doc[labels.SERVICE_ROUTES]) == 1
    if username is not None:
        assert doc[labels.SERVICE_USER] == username
    else:
        assert labels.SERVICE_USER not in doc
