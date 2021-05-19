# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for helper methods of the storage volume base module."""

import json
import os
import pytest

from flowserv.model.files import io_file
from flowserv.volume.fs import FileSystemStorage


@pytest.mark.parametrize('verbose', [True, False])
def test_copy_files(verbose, basedir, emptydir, filenames_all):
    """Run file copy with the two options for the verbose flag."""
    source = FileSystemStorage(basedir=basedir)
    target = FileSystemStorage(basedir=emptydir)
    source.copy(src=None, dst=None, store=target, verbose=verbose)
    files = {key: file for key, file in target.walk(src='')}
    assert set(files.keys()) == filenames_all


def test_storage_folder(tmpdir):
    """Test the storage folder helper class."""
    folder = FileSystemStorage(basedir=os.path.join(tmpdir, 'data'))
    folder = FileSystemStorage(basedir=os.path.join(tmpdir, 'data'))
    folder.store(file=io_file({'a': 1}), dst='a.json')
    assert os.path.isfile(os.path.join(tmpdir, 'data', 'a.json'))
    with folder.load('a.json').open() as f:
        doc = json.load(f)
    assert doc == {'a': 1}
