# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for flowServ applications."""

import os
import pytest

from flowserv.app.base import App, install_app, uninstall_app
from flowserv.config.api import FLOWSERV_API_BASEDIR
from flowserv.config.database import FLOWSERV_DB
from flowserv.model.files.fs import FileSystemStore
from flowserv.tests.controller import StateEngine
from flowserv.tests.files import DiskStore

import flowserv.error as err


DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../.files/benchmark/helloworld')


@pytest.mark.parametrize('fscls', [FileSystemStore, DiskStore])
def test_install_app(fscls, database, tmpdir):
    """Install a workflow template as a flowServ application."""
    fs = fscls(basedir=tmpdir)
    app_key = install_app(source=TEMPLATE_DIR, db=database, fs=fs)
    assert app_key is not None
    # Get the App instance.
    app = App(db=database, engine=StateEngine(), fs=fs, key=app_key)
    assert app.name() is not None
    assert app.description() is None
    assert app.instructions() is None
    assert len(app.parameters()) == 3


def test_install_app_from_env(tmpdir):
    """Install a workflow template as a flowServ application from the settings
    in the environment variables.
    """
    os.environ[FLOWSERV_DB] = 'sqlite:///{}/flowserv.db'.format(str(tmpdir))
    os.environ[FLOWSERV_API_BASEDIR] = str(tmpdir)
    from flowserv.service.database import database
    database.__init__()
    database.init()
    app_key = install_app(source=TEMPLATE_DIR)
    assert app_key is not None
    # Get the App instance.
    app = App(key=app_key)
    assert app.name() is not None
    assert app.description() is None
    assert app.instructions() is None
    assert len(app.parameters()) == 3
    # Clean-up
    del os.environ[FLOWSERV_DB]
    del os.environ[FLOWSERV_API_BASEDIR]


@pytest.mark.parametrize('fscls', [FileSystemStore, DiskStore])
def test_uninstall_app(fscls, database, tmpdir):
    """Uninstall a workflow template as a flowServ application."""
    fs = fscls(basedir=tmpdir)
    app_key = install_app(source=TEMPLATE_DIR, db=database, fs=fs)
    assert app_key is not None
    App(db=database, engine=StateEngine(), fs=fs, key=app_key)
    uninstall_app(app_key=app_key, db=database, fs=fs)
    with pytest.raises(err.UnknownWorkflowError):
        App(key=app_key)


def test_uninstall_app_from_env(tmpdir):
    """Uninstall a workflow template as a flowServ application that has been
    installed from the settings in the environment variables.
    """
    # -- Setup ----------------------------------------------------------------
    os.environ[FLOWSERV_DB] = 'sqlite:///{}/flowserv.db'.format(str(tmpdir))
    os.environ[FLOWSERV_API_BASEDIR] = str(tmpdir)
    from flowserv.service.database import database
    database.__init__()
    database.init()
    # -- Install and uninstall app --------------------------------------------
    app_key = install_app(source=TEMPLATE_DIR)
    assert app_key is not None
    App(key=app_key)
    uninstall_app(app_key=app_key)
    with pytest.raises(err.UnknownWorkflowError):
        App(key=app_key)
    # -- Cleanup --------------------------------------------------------------
    del os.environ[FLOWSERV_DB]
    del os.environ[FLOWSERV_API_BASEDIR]
