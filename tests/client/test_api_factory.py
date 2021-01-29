# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the client API factory."""

import os
import pytest

from flowserv.client.api import ClientAPI

import flowserv.config as config


def test_invalid_factory_type():
    """Test error when attempting to create API factory for an invalid client
    type.
    """
    os.environ[config.FLOWSERV_CLIENT] = 'UNKNOWN'
    with pytest.raises(ValueError):
        ClientAPI()
    del os.environ[config.FLOWSERV_CLIENT]
