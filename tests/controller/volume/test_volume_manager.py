# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the volume manager."""

import os
import pytest

from flowserv.controller.volume.fs import FileSystemStorage
from flowserv.controller.volume.manager import VolumeManager, DEFAULT_STORE

import flowserv.error as err


def test_manager_init(tmpdir):
    """Test edge cases for the volume manager initialization."""
    # Ensure we can instantiate the volume manager if a default store is given.
    volume = VolumeManager([FileSystemStorage(basedir=tmpdir, identifier=DEFAULT_STORE)])
    assert volume.files == dict()
    volume = VolumeManager(
        stores=[FileSystemStorage(basedir=tmpdir, identifier=DEFAULT_STORE)],
        staticfiles={'f1': [DEFAULT_STORE]}
    )
    assert volume.files == {'f1': [DEFAULT_STORE]}
    # Error cases when no default store is given.
    with pytest.raises(ValueError):
        VolumeManager([])
    with pytest.raises(ValueError):
        VolumeManager([FileSystemStorage(basedir=tmpdir, identifier='unknown')])
    # Error for unknown storage volume.
    with pytest.raises(err.UnknownObjectError):
        VolumeManager(
            stores=[FileSystemStorage(basedir=tmpdir, identifier=DEFAULT_STORE)],
            staticfiles={'f1': ['unknown']}
        )


def test_manager_prepare(tmpdir):
    """Test the volume manager prepare method."""
    # -- Setup ----------------------------------------------------------------
    volume = VolumeManager(
        stores=[
            FileSystemStorage(basedir=os.path.join(tmpdir, 's0'), identifier=DEFAULT_STORE),
            FileSystemStorage(basedir=os.path.join(tmpdir, 's1'), identifier='s1'),
            FileSystemStorage(basedir=os.path.join(tmpdir, 's2'), identifier='s2')
        ],
        staticfiles={'f1': [DEFAULT_STORE]}
    )
    filename = os.path.join(tmpdir, 's0', 'f1')
    with open(filename, 'w') as f:
        f.write('Hello World\n')
    # Case 1: Empty arguments.
    assert volume.prepare(files=[]) == dict()
    # Case 2: No file copy.
    files = volume.prepare(files=['f1'], stores=[DEFAULT_STORE])
    assert len(files) == 1 and 'f1' in files and files['f1'].identifier == DEFAULT_STORE
    # Case 3: Copy file between stores.
    files = volume.prepare(files=['f1'], stores=['s1'])
    assert len(files) == 1 and 'f1' in files and files['f1'].identifier == 's1'
    assert volume.files == {'f1': [DEFAULT_STORE, 's1']}
    # Case 4: Copy files between volumes.
    filename = os.path.join(tmpdir, 's1', 'f2')
    with open(filename, 'w') as f:
        f.write('Hello World\n')
    volume.update(files=['f2'], store='s1')
    files = volume.prepare(files=['f2'], stores=['s2'])
    assert len(files) == 1 and 'f2' in files and files['f2'].identifier == 's2'
    assert volume.files == {'f1': [DEFAULT_STORE, 's1'], 'f2': ['s1', DEFAULT_STORE, 's2']}
    for filename in [os.path.join(s, f) for s, f in [('s1', 'f1'), ('s2', 'f2')]]:
        with open(os.path.join(tmpdir, filename), 'rt') as f:
            lines = [line.strip() for line in f]
        assert lines == ['Hello World']
    # Error cases.
    with pytest.raises(err.UnknownFileError):
        volume.prepare(files=['unknown'])
    with pytest.raises(err.UnknownObjectError):
        volume.prepare(files=['f1'], stores=['s1', 'unknown'])


def test_manager_update(tmpdir):
    """Test the update method for the volume manager."""
    volume = VolumeManager(
        stores=[
            FileSystemStorage(basedir=tmpdir, identifier=DEFAULT_STORE),
            FileSystemStorage(basedir=tmpdir, identifier='s1')
        ],
        staticfiles={'f1': [DEFAULT_STORE]}
    )
    assert volume.files == {'f1': [DEFAULT_STORE]}
    volume.update(files=['f1', 'f2'], store='s1')
    assert volume.files == {'f1': ['s1'], 'f2': ['s1']}
    volume.update(files=['f2'])
    assert volume.files == {'f1': ['s1'], 'f2': [DEFAULT_STORE]}
    volume.update(files=['f2'], store='s1')
    assert volume.files == {'f1': ['s1'], 'f2': ['s1']}
    with pytest.raises(ValueError):
        volume.update(files=['f2'], store='s2')
