# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the valid name constraint."""

import pytest

from flowserv.model.constraint import validate_name

import flowserv.error as err


@pytest.mark.parametrize('name', [None, '', ' ', '\t', 'X'*600])
def test_invalid_names(name):
    """Test exception for invalid names."""
    with pytest.raises(err.ConstraintViolationError):
        validate_name(name)


@pytest.mark.parametrize('name', ['A', ' A', 'A_B-C'])
def test_valid_names(name):
    """Test no exception for valid names."""
    validate_name(name)
