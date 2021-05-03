# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the valid name constraint."""

import pytest

from flowserv.model.constraint import validate_identifier, validate_name

import flowserv.error as err


@pytest.mark.parametrize(
    'identifier,valid',
    [
        (None, True),
        ('', False),
        ('12345', True),
        ('aAfshdksdfhgksdfjh_5849', True),
        ('__aAfshdksdfhgks___jh_5849', True),
        ('aAfshdksdfhgksdfjh-5849', False),
        ('.', False),
        ('aAfshdksdfhgksdfjh#$%5849', False),
        ('a' * 32, True),
        ('a' * 33, False)
    ]
)
def test_valid_workflow_identifier(identifier, valid):
    """Test function that validates user-provided workflow identifier."""
    if not valid:
        # If an identifier is invalid a ValueError will be raised.
        with pytest.raises(ValueError):
            validate_identifier(identifier)
    else:
        # If an identifier is valid no exception is raised and the resutl will
        # be True.
        assert validate_identifier(identifier)


@pytest.mark.parametrize('name', [None, '', ' ', '\t', 'X' * 600])
def test_invalid_names(name):
    """Test exception for invalid names."""
    with pytest.raises(err.ConstraintViolationError):
        validate_name(name)


@pytest.mark.parametrize('name', ['A', ' A', 'A_B-C'])
def test_valid_names(name):
    """Test no exception for valid names."""
    validate_name(name)
