# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for the API service descriptor view."""

from flowserv.service.descriptor import ServiceDescriptor, SERVICE_DESCRIPTOR
from flowserv.view.validate import validator

import flowserv.config as config
import flowserv.service.descriptor as route


def test_service_descriptor():
    """Test the service descriptor object."""
    schema = validator('ServiceDescriptor')
    # Local service descriptor (no arguments).
    service = ServiceDescriptor.from_config(env=config.env())
    schema.validate(service.to_dict())
    assert service.routes().get(SERVICE_DESCRIPTOR) is not None
    assert service.routes().get('foo') is None
    # Remote service descriptor (init from local serialization).
    service = ServiceDescriptor(doc=service.to_dict(), routes={'foo': 'bar'})
    schema.validate(service.to_dict())
    assert service.routes().get(SERVICE_DESCRIPTOR) is not None
    schema.validate(service.to_dict())
    assert service.routes().get('foo') == 'bar'


def test_service_descriptor_urls():
    """Test getting Urls from the service descriptor."""
    service = ServiceDescriptor.from_config(env=config.env())
    doc = service.to_dict()
    doc['url'] = 'http://localhost//'
    service = ServiceDescriptor(doc=doc)
    assert service.urls(route.USERS_ACTIVATE) == 'http://localhost/users/activate'
    url = service.urls(route.FILES_DELETE, fileId='F1', userGroupId='G0')
    assert url == 'http://localhost/uploads/G0/files/F1'
