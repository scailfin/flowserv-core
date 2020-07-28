# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for flowServ applications."""

import os

from flowserv.app import install_app, list_apps
from flowserv.config.api import FLOWSERV_API_BASEDIR
from flowserv.config.database import FLOWSERV_DB


DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../.files/benchmark/helloworld')


def test_install_app(database, tmpdir):
    """Install a workflow template as a flowServ application."""
    app_key = install_app(sourcedir=TEMPLATE_DIR, db=database, basedir=tmpdir)
    assert app_key is not None
    apps = list_apps(db=database)
    assert len(apps) == 1
    assert apps[0][1] == app_key


def test_install_app_from_env(tmpdir):
    """Install a workflow template as a flowServ application from the settings
    in the environment variables.
    """
    os.environ[FLOWSERV_DB] = 'sqlite:///{}/flowserv.db'.format(str(tmpdir))
    os.environ[FLOWSERV_API_BASEDIR] = str(tmpdir)
    from flowserv.service.database import database
    database.init()
    app_key = install_app(sourcedir=TEMPLATE_DIR,)
    assert app_key is not None
    apps = list_apps()
    assert len(apps) == 1
    assert apps[0][1] == app_key
    del os.environ[FLOWSERV_DB]
    del os.environ[FLOWSERV_API_BASEDIR]
