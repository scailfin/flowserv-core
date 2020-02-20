# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for file descriptors, file handles, and input files."""

import os
import pytest

from datetime import datetime

from flowserv.core.files import FileDescriptor, FileHandle, InputFile

import flowserv.core.error as err
import flowserv.core.util as util


def test_file_descriptor():
    """Test creating file desriptor class objects."""
    fd = FileDescriptor(
        identifier='ABC',
        name='My Name',
        created_at=datetime.now()
    )
    assert fd.identifier == 'ABC'
    assert fd.name == 'My Name'
    assert fd.created_at_local_time() is not None


def test_file_handle(tmpdir):
    """Test properties and methods of the file handle."""
    filename = os.path.join(str(tmpdir), 'myfile.json')
    util.write_object(filename=filename, obj={'A': 1})
    fh = FileHandle(filename=filename)
    assert fh.identifier is not None
    assert fh.created_at is not None
    assert fh.size > 0
    assert fh.name == 'myfile.json'
    assert fh.mimetype == 'application/json'
    assert fh.path == fh.filename
    assert os.path.isfile(fh.path)
    # Provide alternative file name
    fh1 = FileHandle(filename=filename, identifier='ABC', name='XYZ')
    assert fh1.identifier == 'ABC'
    assert fh1.created_at == fh.created_at
    assert fh1.size == fh.size
    assert fh1.name == 'XYZ'
    fh.delete()
    assert not os.path.isfile(fh.path)
    # Deleting a non-existing file does not raise an error
    fh.delete()
    fh1.delete()
    # Directory handle
    dirpath = os.path.join(str(tmpdir), 'data')
    os.makedirs(dirpath)
    dh = FileHandle(filename=dirpath)
    assert dh.identifier is not None
    assert dh.created_at is not None
    assert dh.size > 0
    assert dh.name == 'data'
    assert dh.mimetype is None
    assert os.path.isdir(dh.path)
    dh.delete()
    assert not os.path.isdir(dh.path)
    # Error if the file does not exist
    with pytest.raises(err.UnknownFileError):
        FileHandle(filename=os.path.join(str(tmpdir), 'not-myfile.json'))


def test_source_and_target_path(tmpdir):
    """Test source and target path methods for input file handle."""
    filename = os.path.join(str(tmpdir), 'myfile.json')
    util.write_object(filename=filename, obj={'A': 1})
    fh = FileHandle(filename=filename)
    # Input file handle without target path
    f = InputFile(f_handle=fh)
    assert f.source() == filename
    assert f.target() == 'myfile.json'
    # Input file handle with target path
    f = InputFile(f_handle=fh, target_path='data/names.txt')
    assert f.source() == filename
    assert f.target() == 'data/names.txt'
