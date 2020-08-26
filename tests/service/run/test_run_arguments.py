# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for run argument helper classes."""

import pytest

from flowserv.service.run.argument import ARG, GET_ARG
from flowserv.service.run.argument import FILE, GET_FILE, IS_FILE


def test_check_file_argument():
    """Test checking an argument value for representing an input file."""
    assert IS_FILE(FILE('0000'))
    assert not IS_FILE({'id': 'names'})
    assert not IS_FILE({'dtype': '$record', 'value': []})


def test_deserialize_file_argument():
    """Test deserialization of file arguments."""
    file_id, target = GET_FILE(FILE('0000', 'names.txt'))
    assert file_id == '0000'
    assert target == 'names.txt'
    file_id, target = GET_FILE(FILE('0000'))
    assert file_id == '0000'
    assert target is None
    with pytest.raises(ValueError):
        GET_FILE({'fileId': '0000'})


def test_deserialize_run_argument():
    """Test deserialization of run arguments."""
    key, value = GET_ARG(ARG('names', 'names.txt'))
    assert key == 'names'
    assert value == 'names.txt'
    with pytest.raises(ValueError):
        GET_ARG({'id': 'names'})
