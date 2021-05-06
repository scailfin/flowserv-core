# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the file system storage volume manager."""

import json
import os
import pytest

from flowserv.volume.fs import FileSystemStorage, walkdir

import flowserv.error as err
import flowserv.util as util


def test_fs_volume_download_all(basedir, emptydir, filenames_all, data_a):
    """Test downloading the full directory of a storage volume."""
    source = FileSystemStorage(basedir=basedir)
    target = FileSystemStorage(basedir=emptydir)
    source.download(src=None, store=target)
    files = {key: file for key, file in target.walk(src='')}
    assert set(files.keys()) == filenames_all
    with files['A.json'].open() as f:
        assert json.load(f) == data_a


def test_fs_volume_download_file(basedir, emptydir, data_e):
    """Test downloading a file from a storage volume."""
    source = FileSystemStorage(basedir=basedir)
    target = FileSystemStorage(basedir=emptydir)
    source.download(src='examples/data/data.json', store=target)
    files = {key: file for key, file in target.walk(src='')}
    assert set(files.keys()) == {'examples/data/data.json'}
    with files['examples/data/data.json'].open() as f:
        assert json.load(f) == data_e


def test_fs_volume_erase(basedir):
    """Test erasing the file system storage volume."""
    store = FileSystemStorage(basedir=basedir)
    store.erase()
    assert not os.path.isdir(basedir)


def test_fs_volume_init(basedir):
    """Test initializing the file system storage volume."""
    store = FileSystemStorage(basedir=basedir)
    assert store.identifier is not None
    assert basedir in store.describe()
    store.close()
    store = FileSystemStorage(basedir=basedir, identifier='0000')
    assert store.identifier == '0000'
    store.close()


def test_fs_volume_load_file(basedir, data_e):
    """Test loading a file from a file system storage volume."""
    store = FileSystemStorage(basedir=basedir)
    with store.load(key='examples/data/data.json').open() as f:
        doc = json.load(f)
    assert doc == data_e
    # -- Error case for unknown file.
    with pytest.raises(err.UnknownFileError):
        store.load(key='examples/data/unknown.json')


def test_fs_volume_upload_all(basedir, emptydir, filenames_all, data_a):
    """Test uploading a full directory to a storage volume."""
    source = FileSystemStorage(basedir=basedir)
    target = FileSystemStorage(basedir=emptydir)
    target.upload(src=None, store=source)
    files = {key: file for key, file in target.walk(src='')}
    assert set(files.keys()) == filenames_all
    with files['A.json'].open() as f:
        assert json.load(f) == data_a


def test_fs_volume_upload_file(basedir, emptydir, data_e):
    """Test uploading a file to a storage volume."""
    source = FileSystemStorage(basedir=basedir)
    target = FileSystemStorage(basedir=emptydir)
    target.upload(src='examples/data/data.json', store=source)
    files = {key: file for key, file in target.walk(src='')}
    assert set(files.keys()) == {'examples/data/data.json'}
    with files['examples/data/data.json'].open() as f:
        assert json.load(f) == data_e


def test_fs_volume_walk(basedir, filenames_all):
    """Test listing files in a directory."""
    store = FileSystemStorage(basedir=basedir)
    # -- Full directory.
    files = store.walk(src='')
    assert set([key for key, _ in files]) == filenames_all
    # -- Sub-directory.
    files = store.walk(src='examples')
    keys = set([key for key, _ in files])
    assert keys == {'examples/B.json', 'examples/C.json', 'examples/data/data.json'}
    files = store.walk(src=util.join('examples', 'data'))
    assert set([key for key, _ in files]) == {'examples/data/data.json'}
    # -- Single file.
    files = store.walk(src=util.join('docs', 'D.json'))
    assert set([key for key, _ in files]) == {'docs/D.json'}
    # -- Unknown file or directory.
    files = store.walk(src=util.join('docs', 'E.json'))
    assert files == []


def test_fs_walkdir(basedir, filenames_all):
    """Test walk for file system directories."""
    # Add an empty directory (that should not be included in the result).
    os.makedirs(os.path.join(basedir, 'ignoreme'))
    files = walkdir(dirname=basedir, prefix=None, files=list())
    assert set([key for key, _ in files]) == filenames_all
