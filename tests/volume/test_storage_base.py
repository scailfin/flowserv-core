# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for helper methods of the storage volume base module."""

import pytest

from flowserv.volume.fs import FileSystemStorage


@pytest.mark.parametrize('verbose', [True, False])
def test_copy_files(verbose, basedir, emptydir, filenames_all):
    """Ru file copye with the two options for the verbose flag."""
    source = FileSystemStorage(basedir=basedir)
    target = FileSystemStorage(basedir=emptydir)
    source.download(src=None, store=target, verbose=verbose)
    files = {key: file for key, file in target.walk(src='')}
    assert set(files.keys()) == filenames_all
