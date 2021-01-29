# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for run argument helper classes."""

import pytest

from flowserv.service.run.argument import serialize_arg, deserialize_arg
from flowserv.service.run.argument import serialize_fh, deserialize_fh, is_fh


def test_check_file_argument():
    """Test checking an argument value for representing an input file."""
    assert is_fh(serialize_fh('0000'))
    assert not is_fh({'id': 'names'})
    assert not is_fh({'dtype': '$record', 'value': []})


def test_deserialize_file_argument():
    """Test deserialization of file arguments."""
    file_id, target = deserialize_fh(serialize_fh('0000', 'names.txt'))
    assert file_id == '0000'
    assert target == 'names.txt'
    file_id, target = deserialize_fh(serialize_fh('0000'))
    assert file_id == '0000'
    assert target is None
    with pytest.raises(ValueError):
        deserialize_fh({'fileId': '0000'})


def test_deserialize_run_argument():
    """Test deserialization of run arguments."""
    key, value = deserialize_arg(serialize_arg('names', 'names.txt'))
    assert key == 'names'
    assert value == 'names.txt'
    with pytest.raises(ValueError):
        deserialize_arg({'id': 'names'})
