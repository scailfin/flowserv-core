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

import flowserv.util as util


@pytest.mark.parametrize('verbose', [True, False])
def test_copy_files_to_root(verbose, basedir, emptydir, filenames_all):
    """Run file copy with the two options for the verbose flag."""
    source = FileSystemStorage(basedir=basedir)
    target = FileSystemStorage(basedir=emptydir)
    # Copy to root folder.
    files = source.copy(src=None, dst=None, store=target, verbose=verbose)
    assert set(files) == filenames_all
    assert {key for key, _ in target.walk(src='')} == filenames_all


def test_copy_files_to_subdir(basedir, emptydir, filenames_all):
    """Copy files to a sub-folder in the target store."""
    source = FileSystemStorage(basedir=basedir)
    target = FileSystemStorage(basedir=emptydir)
    files = source.copy(src=None, dst='sub/dir', store=target)
    filenames_all = {util.join('sub', 'dir', key) for key in filenames_all}
    assert set(files) == filenames_all
    assert {key for key, _ in target.walk(src='')} == filenames_all


def test_storage_folder(tmpdir):
    """Test the storage folder helper class."""
    folder = FileSystemStorage(basedir=os.path.join(tmpdir, 'data'))
    folder = FileSystemStorage(basedir=os.path.join(tmpdir, 'data'))
    folder.store(file=io_file({'a': 1}), dst='a.json')
    assert os.path.isfile(os.path.join(tmpdir, 'data', 'a.json'))
    with folder.load('a.json').open() as f:
        doc = json.load(f)
    assert doc == {'a': 1}
