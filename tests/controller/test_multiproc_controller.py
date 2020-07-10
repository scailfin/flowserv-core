# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the asynchronous multiprocess workflow controller."""

import os
import time

from flowserv.config.api import FLOWSERV_API_BASEDIR
from flowserv.config.database import FLOWSERV_DB
from flowserv.controller.serial.engine import SerialWorkflowEngine
from flowserv.service.api import service
from flowserv.service.run import ARG_ID, ARG_VALUE, ARG_AS
from flowserv.tests.files import FakeStream
from flowserv.tests.service import (
    create_group, create_user, create_workflow, start_run, upload_file
)


import flowserv.model.workflow.state as st


# Template directory
DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../.files/benchmark/helloworld')


def test_cancel_run_helloworld(service):
    """Test cancelling a helloworld run."""
    # -- Setup ----------------------------------------------------------------
    #
    # Start a new run for the workflow template.
    engine = SerialWorkflowEngine(is_async=True)
    with service(engine=engine) as api:
        workflow_id = create_workflow(api, sourcedir=TEMPLATE_DIR)
        user_id = create_user(api)
        group_id = create_group(api, workflow_id, [user_id])
        names = FakeStream(data=['Alice', 'Bob', 'Zoe'], format='plain/text')
        file_id = upload_file(api, group_id, user_id, names)
        args = [
            {ARG_ID: 'names', ARG_VALUE: file_id},
            {ARG_ID: 'sleeptime', ARG_VALUE: 10},
            {ARG_ID: 'greeting', ARG_VALUE: 'Hi'}
        ]
        run_id = start_run(api, group_id, user_id, arguments=args)
    # Poll run after sleeping for one second.
    time.sleep(1)
    with service(engine=engine) as api:
        run = api.runs().get_run(run_id=run_id, user_id=user_id)
    assert run['state'] in st.ACTIVE_STATES
    # -- Cancel the active run ------------------------------------------------
    with service(engine=engine) as api:
        run = api.runs().cancel_run(
            run_id=run_id,
            user_id=user_id,
            reason='done'
        )
        assert run['state'] == st.STATE_CANCELED
        assert run['messages'][0] == 'done'
    with service(engine=engine) as api:
        run = api.runs().get_run(run_id=run_id, user_id=user_id)
        assert run['state'] == st.STATE_CANCELED
        assert run['messages'][0] == 'done'


def test_run_helloworld_async(tmpdir):
    """Execute the helloworld example."""
    # -- Setup ----------------------------------------------------------------
    #
    # Start a new run for the workflow template.
    os.environ[FLOWSERV_DB] = 'sqlite:///{}/flowserv.db'.format(str(tmpdir))
    os.environ[FLOWSERV_API_BASEDIR] = str(tmpdir)
    from flowserv.service.database import database
    database.init()
    engine = SerialWorkflowEngine(is_async=True)
    with service(engine=engine) as api:
        workflow_id = create_workflow(api, sourcedir=TEMPLATE_DIR)
        user_id = create_user(api)
        group_id = create_group(api, workflow_id, [user_id])
        names = FakeStream(data=['Alice', 'Bob', 'Zoe'], format='plain/text')
        file_id = upload_file(api, group_id, user_id, names)
        args = [
            {ARG_ID: 'names', ARG_VALUE: file_id},
            {ARG_ID: 'sleeptime', ARG_VALUE: 1},
            {ARG_ID: 'greeting', ARG_VALUE: 'Hi'}
        ]
        run_id = start_run(api, group_id, user_id, arguments=args)
    # Poll workflow state every second.
    with service(engine=engine) as api:
        run = api.runs().get_run(run_id=run_id, user_id=user_id)
    while run['state'] in st.ACTIVE_STATES:
        time.sleep(1)
        with service(engine=engine) as api:
            run = api.runs().get_run(run_id=run_id, user_id=user_id)
    assert run['state'] == st.STATE_SUCCESS
    files = dict()
    for f in run['files']:
        files[f['name']] = f['id']
    fh = api.runs().get_result_file(
        run_id=run_id,
        file_id=files['results/greetings.txt'],
        user_id=user_id
    )
    with open(fh.filename) as f:
        greetings = f.read()
        assert 'Hi Alice' in greetings
        assert 'Hi Bob' in greetings
        assert 'Hi Zoe' in greetings
    fh = api.runs().get_result_file(
        run_id=run_id,
        file_id=files['results/analytics.json'],
        user_id=user_id
    )
    assert os.path.isfile(fh.filename)
    # Clean-up environment variables
    del os.environ[FLOWSERV_DB]
    del os.environ[FLOWSERV_API_BASEDIR]


def test_run_helloworld_with_missing_file(tmpdir):
    """Execute the helloworld example with a names file that has been copied to
    the wrong location.
    """
    # -- Setup ----------------------------------------------------------------
    #
    # Start a new run for the workflow template.
    os.environ[FLOWSERV_DB] = 'sqlite:///{}/flowserv.db'.format(str(tmpdir))
    os.environ[FLOWSERV_API_BASEDIR] = str(tmpdir)
    from flowserv.service.database import database
    database.init()
    engine = SerialWorkflowEngine(is_async=True)
    with service(engine=engine) as api:
        workflow_id = create_workflow(api, sourcedir=TEMPLATE_DIR)
        user_id = create_user(api)
        group_id = create_group(api, workflow_id, [user_id])
        names = FakeStream(data=['Alice', 'Bob', 'Zoe'], format='plain/text')
        file_id = upload_file(api, group_id, user_id, names)
        args = [
            {ARG_ID: 'names', ARG_VALUE: file_id, ARG_AS: 'somenames.txt'},
            {ARG_ID: 'sleeptime', ARG_VALUE: 1},
            {ARG_ID: 'greeting', ARG_VALUE: 'Hi'}
        ]
        run_id = start_run(api, group_id, user_id, arguments=args)
    # Poll workflow state every second.
    with service(engine=engine) as api:
        run = api.runs().get_run(run_id=run_id, user_id=user_id)
    while run['state'] in st.ACTIVE_STATES:
        time.sleep(1)
        with service(engine=engine) as api:
            run = api.runs().get_run(run_id=run_id, user_id=user_id)
    assert run['state'] == st.STATE_ERROR
    # Clean-up environment variables
    del os.environ[FLOWSERV_DB]
    del os.environ[FLOWSERV_API_BASEDIR]
