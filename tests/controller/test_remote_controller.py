# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the generic remote workflow engine controller."""

import os
import time

from flowserv.config.api import FLOWSERV_API_BASEDIR
from flowserv.config.db import FLOWSERV_DB
from flowserv.model.db import DB
from flowserv.service.api import API
from flowserv.service.run import ARG_ID, ARG_VALUE
from flowserv.tests.files import FakeStream
from flowserv.tests.remote import RemoteTestController
from flowserv.tests.service import (
    create_group, create_user, create_workflow, start_run, upload_file
)

import flowserv.model.workflow.state as st
import flowserv.tests.serialize as serialize


# Template directory
DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../.files/benchmark/helloworld')


def test_run_remote_workflow(tmpdir):
    """Execute the helloworld example."""
    # -- Setup ----------------------------------------------------------------
    # Need to construct the database and API manually to ensure that we use
    # the same database when the remote client needs to update the run state.
    os.environ[FLOWSERV_DB] = 'sqlite:///{}/foo.db'.format(str(tmpdir))
    os.environ[FLOWSERV_API_BASEDIR] = str(tmpdir)
    db = DB()
    db.init()
    api = API(db=db, engine=RemoteTestController())
    # Start a new run for the workflow template.
    workflow_id = create_workflow(api, sourcedir=TEMPLATE_DIR)
    user_id = create_user(api)
    group_id = create_group(api, workflow_id, [user_id])
    names = FakeStream(data=['Alice', 'Bob', 'Zoe'], format='txt/plain')
    file_id = upload_file(api, group_id, user_id, names)
    run_id = start_run(
        api,
        group_id,
        user_id,
        arguments=[
            {ARG_ID: 'names', ARG_VALUE: file_id},
            {ARG_ID: 'sleeptime', ARG_VALUE: 1},
            {ARG_ID: 'greeting', ARG_VALUE: 'Hi'}
        ]
    )
    # Poll workflow state every second.
    run = api.runs().get_run(run_id=run_id, user_id=user_id)
    while run['state'] in st.ACTIVE_STATES:
        time.sleep(1)
        run = api.runs().get_run(run_id=run_id, user_id=user_id)
    serialize.validate_run_handle(run, state=st.STATE_SUCCESS)
    files = dict()
    for obj in run['files']:
        files[obj['name']] = obj['id']
    f_id = files['results/greetings.txt']
    fh = api.runs().get_result_file(
        run_id=run_id,
        file_id=f_id,
        user_id=user_id
    )
    with open(fh.filename) as f:
        greetings = f.read()
        assert 'Hi Alice' in greetings
        assert 'Hi Bob' in greetings
        assert 'Hi Zoe' in greetings
    f_id = files['results/analytics.json']
    fh = api.runs().get_result_file(
        run_id=run_id,
        file_id=f_id,
        user_id=user_id
    )
    assert os.path.isfile(fh.filename)
    # Clean up
    del os.environ[FLOWSERV_DB]
    del os.environ[FLOWSERV_API_BASEDIR]
