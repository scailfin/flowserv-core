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

from io import BytesIO

from flowserv.app.base import App, install_app
from flowserv.config.api import FLOWSERV_API_BASEDIR
from flowserv.config.database import FLOWSERV_DB
from flowserv.config.files import (
    FLOWSERV_FILESTORE_MODULE, FLOWSERV_FILESTORE_CLASS
)
from flowserv.model.files.fs import FileSystemStore
from flowserv.model.files.s3 import FLOWSERV_S3BUCKET

from flowserv.tests.controller import StateEngine

import flowserv.model.workflow.state as state
import flowserv.util as util


DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../.files/benchmark/helloworld')


def test_run_app(database, tmpdir):
    """Simulate running the test workflow app."""
    fs = FileSystemStore(basedir=tmpdir)
    app_key = install_app(source=TEMPLATE_DIR, db=database, fs=fs)
    engine = StateEngine()
    app = App(db=database, engine=engine, fs=fs, key=app_key)
    r = app.run({'names': BytesIO(b'Alice'), 'sleeptime': 0, 'greeting': 'Hi'})
    assert r['state'] == state.STATE_PENDING


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
    os.environ[FLOWSERV_DB] = 'sqlite:///{}/flowserv.db'.format(str(tmpdir))
    os.environ[FLOWSERV_API_BASEDIR] = str(tmpdir)
    os.environ[FLOWSERV_FILESTORE_MODULE] = fsconfig[FLOWSERV_FILESTORE_MODULE]
    os.environ[FLOWSERV_FILESTORE_CLASS] = fsconfig[FLOWSERV_FILESTORE_CLASS]
    if FLOWSERV_S3BUCKET in os.environ:
        del os.environ[FLOWSERV_S3BUCKET]
    from flowserv.service.database import database
    database.init()
    app_key = install_app(source=TEMPLATE_DIR)
    # -- Run workflow ---------------------------------------------------------
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
    text = util.read_text(file=filename).strip()
    assert text == 'Hi Alice!'
    # -- Clean-up -------------------------------------------------------------
    del os.environ[FLOWSERV_DB]
    del os.environ[FLOWSERV_API_BASEDIR]
    del os.environ[FLOWSERV_FILESTORE_MODULE]
    del os.environ[FLOWSERV_FILESTORE_CLASS]
