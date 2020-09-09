# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for flowServ applications."""

import os

from io import BytesIO

from flowserv.app import App, install_app
from flowserv.config.api import FLOWSERV_API_BASEDIR
from flowserv.config.database import FLOWSERV_DB
from flowserv.tests.controller import StateEngine

import flowserv.model.workflow.state as state


DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../.files/benchmark/helloworld')


def test_run_app(database, tmpdir):
    """Simulate running the test workflow app."""
    app_key = install_app(source=TEMPLATE_DIR, db=database, basedir=tmpdir)
    engine = StateEngine()
    app = App(db=database, engine=engine, basedir=tmpdir, key=app_key)
    r = app.run({'names': BytesIO(b'Alice'), 'sleeptime': 0, 'greeting': 'Hi'})
    assert r['state'] == state.STATE_PENDING


def test_run_app_from_env(tmpdir):
    """Run workflow application that is installed from the settings in the
    environment variables.
    """
    os.environ[FLOWSERV_DB] = 'sqlite:///{}/flowserv.db'.format(str(tmpdir))
    os.environ[FLOWSERV_API_BASEDIR] = str(tmpdir)
    from flowserv.service.database import database
    database.init()
    app_key = install_app(source=TEMPLATE_DIR,)
    app = App(key=app_key)
    r = app.run({'names': BytesIO(b'Alice'), 'sleeptime': 0, 'greeting': 'Hi'})
    assert r['state'] == state.STATE_SUCCESS
    files = dict()
    for obj in r['files']:
        files[obj['name']] = obj['id']
    assert 'results/analytics.json' in files
    file_id = files['results/greetings.txt']
    filename, mimetype = app.get_file(run_id=r['id'], file_id=file_id)
    assert mimetype == 'text/plain'
    with open(filename, 'r') as f:
        text = f.read().strip()
    assert text == 'Hi Alice!'
    # Clean-up
    del os.environ[FLOWSERV_DB]
    del os.environ[FLOWSERV_API_BASEDIR]
