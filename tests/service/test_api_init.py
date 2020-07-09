# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for API methods."""

import pytest

from flowserv.model.auth import DefaultAuthPolicy

import flowserv.error as err


def test_api_components(api_factory):
    """Test methods to access API components."""
    # The API uses the default authentication handler.
    api = api_factory()
    assert isinstance(api.auth, DefaultAuthPolicy)
    # Error when authenticating unknown user.
    with pytest.raises(err.UnauthenticatedAccessError):
        api.authenticate('0000')
    # Access the different managers to ensure that they are created properly
    # without raising errors.
    assert api.groups() is not None
    assert api.runs() is not None
    assert api.server() is not None
    assert api.uploads() is not None
    assert api.users() is not None
    assert api.workflows() is not None
