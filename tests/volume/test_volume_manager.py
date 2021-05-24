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

from flowserv.volume.fs import FileSystemStorage
from flowserv.volume.manager import VolumeManager, DEFAULT_STORE

import flowserv.error as err


def test_manager_init(tmpdir):
    """Test edge cases for the volume manager initialization."""
    default_store = FileSystemStorage(basedir=tmpdir, identifier=DEFAULT_STORE)
    # Ensure we can instantiate the volume manager if a default store is given.
    volume = VolumeManager(stores={DEFAULT_STORE: default_store.to_dict()})
    assert volume.files == dict()
    volume = VolumeManager(stores={DEFAULT_STORE: default_store.to_dict()}, files={'f1': [DEFAULT_STORE]})
    assert volume.files == {'f1': [DEFAULT_STORE]}
    # Error cases when no default store is given.
    with pytest.raises(ValueError):
        VolumeManager(stores=dict())
    with pytest.raises(ValueError):
        VolumeManager(stores={'unknown': default_store.to_dict()})
    # Error for unknown storage volume.
    with pytest.raises(err.UnknownObjectError):
        VolumeManager(
            stores={DEFAULT_STORE: default_store.to_dict()},
            files={'f1': ['unknown']}
        )


def test_manager_prepare(basedir, filenames_all, data_a, tmpdir):
    """Test the volume manager prepare method."""
    # -- Setup ----------------------------------------------------------------
    s0 = FileSystemStorage(basedir=basedir, identifier=DEFAULT_STORE)
    s1 = FileSystemStorage(basedir=os.path.join(tmpdir, 's1'), identifier='s1')
    volumes = VolumeManager(
        stores={
            s0.identifier: s0.to_dict(),
            s1.identifier: s1.to_dict()
        },
        files={f: [DEFAULT_STORE] for f in filenames_all}
    )
    # Case 1: Empty arguments.
    assert volumes.prepare(files=[]) == dict()
    # Case 2: No file copy.
    files = volumes.prepare(files=['examples/'], stores=[DEFAULT_STORE])
    assert len(files) == 3
    for f in files:
        assert files[f].identifier == DEFAULT_STORE
    # Case 3: Copy file between stores.
    files = volumes.prepare(files=['A.json', 'docs/'], stores=['s1'])
    assert set(files.keys()) == {'A.json', 'docs/D.json'}
    for f in files:
        assert files[f].identifier == 's1'
        filename = os.path.join(tmpdir, 's1', 'A.json')
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
    # Error cases.
    with pytest.raises(err.UnknownObjectError):
        volumes.prepare(files=['A.json'], stores=['s1', 'unknown'])


def test_manager_update(tmpdir):
    """Test the update method for the volume manager."""
    doc = FileSystemStorage(basedir=tmpdir, identifier=DEFAULT_STORE).to_dict()
    volume = VolumeManager(stores={DEFAULT_STORE: doc, 's1': doc}, files={'f1': [DEFAULT_STORE]})
    assert volume.files == {'f1': [DEFAULT_STORE]}
    volume.update(files=['f1', 'f2'], store='s1')
    assert volume.files == {'f1': ['s1'], 'f2': ['s1']}
    volume.update(files=['f2'])
    assert volume.files == {'f1': ['s1'], 'f2': [DEFAULT_STORE]}
    volume.update(files=['f2'], store='s1')
    assert volume.files == {'f1': ['s1'], 'f2': ['s1']}
    with pytest.raises(ValueError):
        volume.update(files=['f2'], store='s2')
