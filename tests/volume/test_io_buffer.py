# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the file IO buffer."""

from flowserv.tests.files import io_file


def test_io_buffer_size():
    """Test size method of IOBuffer objects."""
    assert io_file(['Alice', 'Bob']).size() > 0
