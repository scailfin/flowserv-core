# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the client API factory."""

import os
import pytest

from flowserv.client.api import api_factory
from flowserv.config.api import FLOWSERV_API_BASEDIR
from flowserv.config.client import FLOWSERV_CLIENT, LOCAL_CLIENT


def test_invalid_factory_type():
    """Test error when attempting to create API factory for an invalid client
    type.
    """
    with pytest.raises(ValueError):
        api_factory({FLOWSERV_CLIENT: 'UNKNOWN'})


def test_local_api_factory(tmpdir):
    """Test creating an instance of the local service API factory."""
    service = api_factory({
        FLOWSERV_CLIENT: LOCAL_CLIENT,
        FLOWSERV_API_BASEDIR: os.path.abspath(tmpdir)
    })
    with service.api() as api:
        doc = api.workflows().list_workflows()
    assert doc is not None
