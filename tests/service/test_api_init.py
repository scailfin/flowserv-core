# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for API methods."""


def test_api_components(local_service):
    """Test methods to access API components."""
    with local_service() as api:
        # Access the different managers to ensure that they are created
        # properly without raising errors.
        assert api.groups() is not None
        assert api.runs() is not None
        assert api.server() is not None
        assert api.uploads() is not None
        assert api.users() is not None
        assert api.workflows() is not None
