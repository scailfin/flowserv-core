# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the service descriptor view."""

import pytest

from flowserv.view.descriptor import ServiceDescriptorSerializer
from flowserv.view.validate import validator


@pytest.mark.parametrize('username', ['alice', None])
def test_service_descriptor_serialization(username):
    """Validate the serialization of the service descriptor."""
    schema = validator('ServiceDescriptor')
    serializer = ServiceDescriptorSerializer()
    doc = serializer.service_descriptor(
        name='Test',
        version='0.0.0',
        url='http://localhost:5000',
        routes={'home': '/'},
        username=username
    )
    schema.validate(doc)
    assert serializer.get_name(doc) == 'Test'
    assert serializer.get_version(doc) == '0.0.0'
    assert serializer.get_url(doc) == 'http://localhost:5000'
    assert serializer.get_username(doc) == username
    assert serializer.get_routes(doc, {'foo': 'bar'}) == {'home': '/', 'foo': 'bar'}
