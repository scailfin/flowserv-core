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

from io import StringIO

from flowserv.app.base import App, install_app
from flowserv.config.auth import FLOWSERV_AUTH, OPEN_ACCESS
from flowserv.config.api import FLOWSERV_API_BASEDIR
from flowserv.config.controller import FLOWSERV_ASYNC
from flowserv.config.database import FLOWSERV_DB
from flowserv.config.files import (
    FLOWSERV_FILESTORE_MODULE, FLOWSERV_FILESTORE_CLASS
)
from flowserv.model.auth import open_access
from flowserv.model.files.fs import FileSystemStore
from flowserv.model.files.s3 import FLOWSERV_S3BUCKET

from flowserv.tests.controller import StateEngine

import flowserv.model.workflow.state as st


DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../.files/benchmark/helloworld')


def test_app_start_run(database, tmpdir):
    """Simulate running the test workflow app."""
    fs = FileSystemStore(basedir=tmpdir)
    app_key = install_app(source=TEMPLATE_DIR, db=database, fs=fs)
    engine = StateEngine()
    app = App(db=database, engine=engine, fs=fs, auth=open_access, key=app_key)
    # -- Pending run ----------------------------------------------------------
    r = app.start_run({
        'names': StringIO('Alice'),
        'sleeptime': 0,
        'greeting': 'Hi'
    })
    assert r.is_pending()
    # Result files for active runs are non-existent.
    file_id = r.get_file_id(key='results/greetings.txt', raise_error=False)
    assert file_id is None
    with pytest.raises(ValueError):
        r.get_file_id(key='results/greetings.txt')


def test_run_app_error(database, tmpdir):
    """Simulate running the test workflow app that results in a workflow in
    error state.
    """
    fs = FileSystemStore(basedir=tmpdir)
    app_key = install_app(source=TEMPLATE_DIR, db=database, fs=fs)
    engine = StateEngine(state=st.StatePending().error(messages=['The error']))
    app = App(db=database, engine=engine, fs=fs, auth=open_access, key=app_key)
    # -- Pending run ----------------------------------------------------------
    r = app.start_run({
        'names': StringIO('Alice'),
        'sleeptime': 0,
        'greeting': 'Hi'
    })
    assert r.is_error()
    assert r.messages() == ['The error']
    # Result files for error runs are non-existent.
    file_id = r.get_file_id(key='results/greetings.txt', raise_error=False)
    assert file_id is None
    with pytest.raises(ValueError):
        r.get_file_id(key='results/greetings.txt')


@pytest.mark.parametrize(
    'fsconfig',
    [{
        FLOWSERV_FILESTORE_MODULE: 'flowserv.model.files.fs',
        FLOWSERV_FILESTORE_CLASS: 'FileSystemStore'
    }, {
        FLOWSERV_FILESTORE_MODULE: 'flowserv.model.files.s3',
        FLOWSERV_FILESTORE_CLASS: 'BucketStore'
    }]
)
def test_run_app_from_env(fsconfig, tmpdir):
    """Run workflow application that is installed from the settings in the
    environment variables.
    """
    # -- Setup ----------------------------------------------------------------
    os.environ[FLOWSERV_DB] = 'sqlite:///{}/db/flowserv.db'.format(str(tmpdir))
    os.environ[FLOWSERV_API_BASEDIR] = str(tmpdir)
    os.environ[FLOWSERV_AUTH] = OPEN_ACCESS
    os.environ[FLOWSERV_ASYNC] = 'False'
    os.environ[FLOWSERV_FILESTORE_MODULE] = fsconfig[FLOWSERV_FILESTORE_MODULE]
    os.environ[FLOWSERV_FILESTORE_CLASS] = fsconfig[FLOWSERV_FILESTORE_CLASS]
    if FLOWSERV_S3BUCKET in os.environ:
        del os.environ[FLOWSERV_S3BUCKET]
    from flowserv.service.database import database
    database.__init__()
    database.init()
    app_key = install_app(source=TEMPLATE_DIR)
    # -- Run workflow ---------------------------------------------------------
    app = App(key=app_key)
    r = app.start_run({
        'names': StringIO('Alice'),
        'sleeptime': 0,
        'greeting': 'Hi'
    })
    # Run states.
    assert r.is_success()
    assert not r.is_canceled()
    assert not r.is_error()
    assert not r.is_pending()
    assert not r.is_running()
    # Result files.
    file_id = r.get_file_id(key='results/analytics.json')
    assert file_id is not None
    file_id = r.get_file_id(key='results/greetings.txt')
    f = app.get_file(run_id=r.run_id, file_id=file_id)
    assert f.mime_type == 'text/plain'
    text = f.open().read().decode('utf-8').strip()
    assert text == 'Hi Alice!'
    assert r.get_file_id(key='unknown', raise_error=False) is None
    with pytest.raises(ValueError):
        r.get_file_id(key='unknown')
    # -- Clean-up -------------------------------------------------------------
    del os.environ[FLOWSERV_DB]
    del os.environ[FLOWSERV_API_BASEDIR]
    del os.environ[FLOWSERV_AUTH]
    del os.environ[FLOWSERV_ASYNC]
    del os.environ[FLOWSERV_FILESTORE_MODULE]
    del os.environ[FLOWSERV_FILESTORE_CLASS]
