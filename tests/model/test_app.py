# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for flowServ applications."""

import os

from flowserv.app import App, install_app, list_apps, uninstall_app
from flowserv.config.api import FLOWSERV_API_BASEDIR
from flowserv.config.database import FLOWSERV_DB
from flowserv.tests.controller import StateEngine


DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../.files/benchmark/helloworld')


def test_install_app(database, tmpdir):
    """Install a workflow template as a flowServ application."""
    app_key = install_app(source=TEMPLATE_DIR, db=database, basedir=tmpdir)
    assert app_key is not None
    apps = list_apps(db=database)
    assert len(apps) == 1
    assert apps[0][1] == app_key
    # Get the App instance.
    app = App(db=database, engine=StateEngine(), basedir=tmpdir, key=app_key)
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
    database.init()
    app_key = install_app(source=TEMPLATE_DIR,)
    assert app_key is not None
    apps = list_apps()
    assert len(apps) == 1
    assert apps[0][1] == app_key
    # Get the App instance.
    app = App(key=app_key)
    assert app.name() is not None
    assert app.description() is None
    assert app.instructions() is None
    assert len(app.parameters()) == 3
    # Clean-up
    del os.environ[FLOWSERV_DB]
    del os.environ[FLOWSERV_API_BASEDIR]


def test_uninstall_app(database, tmpdir):
    """Uninstall a workflow template as a flowServ application."""
    app_key = install_app(source=TEMPLATE_DIR, db=database, basedir=tmpdir)
    apps = list_apps(db=database)
    assert len(apps) == 1
    uninstall_app(app_key=app_key, db=database, basedir=tmpdir)
    apps = list_apps(db=database)
    assert len(apps) == 0


def test_uninstall_app_from_env(tmpdir):
    """Uninstall a workflow template as a flowServ application that has been
    installed from the settings in the environment variables.
    """
    os.environ[FLOWSERV_DB] = 'sqlite:///{}/flowserv.db'.format(str(tmpdir))
    os.environ[FLOWSERV_API_BASEDIR] = str(tmpdir)
    from flowserv.service.database import database
    database.init()
    app_key = install_app(source=TEMPLATE_DIR,)
    assert app_key is not None
    apps = list_apps()
    assert len(apps) == 1
    uninstall_app(app_key=app_key)
    apps = list_apps()
    assert len(apps) == 0
    del os.environ[FLOWSERV_DB]
    del os.environ[FLOWSERV_API_BASEDIR]
