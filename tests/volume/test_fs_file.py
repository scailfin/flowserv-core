# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the file system IO Handle."""

import os
import pytest

from flowserv.volume.fs import FSFile

import flowserv.error as err


def test_fs_handle_init(basedir):
    """Test initializing FSFile objects."""
    # -- Valid file -----------------------------------------------------------
    FSFile(os.path.join(basedir, 'A.json'))
    # -- Error for unknown file -----------------------------------------------
    with pytest.raises(err.UnknownFileError):
        FSFile(filename=os.path.join(basedir, 'unknown.json'))
    # -- Error for directory --------------------------------------------------
    with pytest.raises(err.UnknownFileError):
        FSFile(filename=os.path.join(basedir, 'docs'))


def test_fs_handle_size(basedir):
    """Test size property of FSFile objects."""
    assert FSFile(os.path.join(basedir, 'A.json')).size() > 0
