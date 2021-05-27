# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for helper methods that are used to configure the serial
workflow engine.
"""

from jsonschema.exceptions import ValidationError

import os
import pytest

from flowserv.controller.serial.engine.base import volume_manager
from flowserv.volume.fs import FileSystemStorage, FStore
from flowserv.volume.manager import DEFAULT_STORE

import flowserv.controller.serial.engine.config as config


# Config files.
DIR = os.path.dirname(os.path.realpath(__file__))
CONFIG_DIR = os.path.join(DIR, '../../../.files/controller')
VALID_CONFIG = os.path.join(CONFIG_DIR, 'worker.json')
INALID_CONFIG = os.path.join(CONFIG_DIR, 'worker.yaml')


def test_engine_config(tmpdir):
    """Test worker configuration for the serial workflow controller."""
    # Missing parameter value.
    assert config.ENGINECONFIG(env=dict()) == {}
    # Read object from file.
    env = {config.FLOWSERV_SERIAL_ENGINECONFIG: VALID_CONFIG}
    assert config.ENGINECONFIG(env=env) is not None
    # Read invalid configuration file.
    env = {config.FLOWSERV_SERIAL_ENGINECONFIG: INALID_CONFIG}
    assert config.ENGINECONFIG(env=env) is not None
    with pytest.raises(ValidationError):
        config.ENGINECONFIG(env=env, validate=True)


def test_engine_rundirectory():
    """Test default run directory path."""
    assert config.RUNSDIR(dict({config.FLOWSERV_SERIAL_RUNSDIR: 'abc'})) == 'abc'
    assert config.RUNSDIR(dict()) is not None


def test_engine_volume_manager(tmpdir):
    """Test creating the volume manager for a workflow run from the engine
    configuration and the default run store.
    """
    runstore = FileSystemStorage(basedir=tmpdir, identifier=DEFAULT_STORE)
    # Minimal arguments.
    volumes = volume_manager(specs=[], runstore=runstore, runfiles=[])
    assert len(volumes._storespecs) == 1
    assert len(volumes.files) == 0
    # Only runstore given.
    volumes = volume_manager(specs=[], runstore=runstore, runfiles=['a', 'b'])
    assert len(volumes._storespecs) == 1
    assert volumes.files['a'] == [DEFAULT_STORE]
    assert volumes.files['b'] == [DEFAULT_STORE]
    # Multiple stores with files.
    doc_ignore = runstore.to_dict()
    doc_ignore['files'] = ['c', 'd']
    doc_fs = FStore(basedir=tmpdir, identifier='s0')
    doc_fs['files'] = ['a', 'c']
    volumes = volume_manager(
        specs=[doc_ignore, doc_fs, FStore(basedir=tmpdir, identifier='s1')],
        runstore=runstore,
        runfiles=['a', 'b']
    )
    assert len(volumes._storespecs) == 3
    assert volumes.files['a'] == [DEFAULT_STORE, 's0']
    assert volumes.files['b'] == [DEFAULT_STORE]
    assert volumes.files['c'] == ['s0']
    assert volumes.files.get('d') is None
