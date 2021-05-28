# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the volume manager."""

import json
import os
import pytest

from flowserv.volume.fs import FileSystemStorage, FStore
from flowserv.volume.manager import VolumeManager, DEFAULT_STORE

import flowserv.error as err


def test_manager_init(tmpdir):
    """Test edge cases for the volume manager initialization."""
    default_store = FStore(basedir=tmpdir, identifier=DEFAULT_STORE)
    # Ensure we can instantiate the volume manager if a default store is given.
    volumes = VolumeManager(stores=[default_store])
    assert volumes.files == dict()
    volumes = VolumeManager(stores=[default_store], files={'f1': [DEFAULT_STORE]})
    assert volumes.files == {'f1': [DEFAULT_STORE]}
    # Error cases when no default store is given.
    with pytest.raises(ValueError):
        VolumeManager(stores=list())
    with pytest.raises(ValueError):
        VolumeManager(stores=[FStore(basedir=tmpdir, identifier='0000')])
    # Error for unknown storage volume.
    with pytest.raises(err.UnknownObjectError):
        VolumeManager(
            stores=[default_store],
            files={'f1': ['unknown']}
        )


def test_manager_prepare(basedir, filenames_all, data_a, tmpdir):
    """Test the volume manager prepare method."""
    # -- Setup ----------------------------------------------------------------
    s1_dir = os.path.join(tmpdir, 's1')
    s0 = FileSystemStorage(basedir=basedir, identifier=DEFAULT_STORE)
    s1 = FileSystemStorage(basedir=s1_dir, identifier='s1')
    volumes = VolumeManager(
        stores=[s0.to_dict(), s1.to_dict()],
        files={f: [DEFAULT_STORE] for f in filenames_all}
    )
    # Case 1: Empty arguments.
    volumes.prepare(store=s0, files=[])
    # Case 2: No file copy.
    volumes.prepare(store=s0, files=['examples/'])
    assert len(os.listdir(basedir)) == 3
    assert len(os.listdir(s1_dir)) == 0
    for f in filenames_all:
        assert volumes.files[f] == [DEFAULT_STORE]
    # Case 3: Copy file between stores.
    volumes.prepare(store=s1, files=['A.json', 'docs/'])
    assert len(os.listdir(basedir)) == 3
    assert len(os.listdir(s1_dir)) == 2
    filename = os.path.join(s1_dir, 'A.json')
    assert os.path.isfile(filename)
    with s1.load('A.json').open() as f:
        assert json.load(f) == data_a
    assert volumes.files == {
        'docs/D.json': [DEFAULT_STORE, 's1'],
        'examples/data/data.json': [DEFAULT_STORE],
        'examples/C.json': [DEFAULT_STORE],
        'A.json': [DEFAULT_STORE, 's1'],
        'examples/B.json': [DEFAULT_STORE]
    }


def test_manager_update(tmpdir):
    """Test the update method for the volume manager."""
    volumes = VolumeManager(
        stores=[
            FStore(basedir=tmpdir, identifier=DEFAULT_STORE),
            FStore(basedir=tmpdir, identifier='s1')
        ],
        files={'f1': [DEFAULT_STORE]}
    )
    default_store = volumes.get(identifier=DEFAULT_STORE)
    s1 = volumes.get(identifier='s1')
    assert volumes.files == {'f1': [DEFAULT_STORE]}
    volumes.update(files=['f1', 'f2'], store=s1)
    assert volumes.files == {'f1': ['s1'], 'f2': ['s1']}
    volumes.update(files=['f2'], store=default_store)
    assert volumes.files == {'f1': ['s1'], 'f2': [DEFAULT_STORE]}
    volumes.update(files=['f2'], store=s1)
    assert volumes.files == {'f1': ['s1'], 'f2': ['s1']}
