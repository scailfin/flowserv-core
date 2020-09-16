# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the asynchronous multiprocess workflow controller."""

import os
import pytest
import time

from flowserv.config.api import FLOWSERV_API_BASEDIR
from flowserv.config.database import FLOWSERV_DB
from flowserv.config.files import (
    FLOWSERV_FILESTORE_MODULE, FLOWSERV_FILESTORE_CLASS
)
from flowserv.controller.serial.engine import SerialWorkflowEngine
from flowserv.service.api import service
from flowserv.service.run.argument import ARG, FILE
from flowserv.tests.files import FakeStream, read_json
from flowserv.tests.service import (
    create_group, create_user, create_workflow, start_run, upload_file
)


import flowserv.model.workflow.state as st
import flowserv.util as util


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
        engine.fs = api.fs
        workflow_id = create_workflow(api, source=TEMPLATE_DIR)
        user_id = create_user(api)
        group_id = create_group(api, workflow_id, [user_id])
        names = FakeStream(data=['Alice', 'Bob', 'Zoe'], format='plain/text')
        file_id = upload_file(api, group_id, user_id, names.save())
        args = [
            ARG('names', FILE(file_id)),
            ARG('sleeptime', 10),
            ARG('greeting', 'Hi')
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


@pytest.mark.parametrize(
    'fsconfig,target',
    [
        (
            {
                FLOWSERV_FILESTORE_MODULE: 'flowserv.model.files.fs',
                FLOWSERV_FILESTORE_CLASS: 'FileSystemStore'
            },
            None
        ),
        (
            {
                FLOWSERV_FILESTORE_MODULE: 'flowserv.model.files.s3',
                FLOWSERV_FILESTORE_CLASS: 'BucketStore'
            },
            'somenames.txt'
        )
    ]
)
def test_run_helloworld_async(fsconfig, target, tmpdir):
    """Execute the helloworld example."""
    # -- Setup ----------------------------------------------------------------
    #
    # Start a new run for the workflow template.
    os.environ[FLOWSERV_DB] = 'sqlite:///{}/flowserv.db'.format(str(tmpdir))
    os.environ[FLOWSERV_API_BASEDIR] = str(tmpdir)
    os.environ[FLOWSERV_FILESTORE_MODULE] = fsconfig[FLOWSERV_FILESTORE_MODULE]
    os.environ[FLOWSERV_FILESTORE_CLASS] = fsconfig[FLOWSERV_FILESTORE_CLASS]
    from flowserv.service.database import database
    database.init()
    engine = SerialWorkflowEngine(is_async=True)
    with service(engine=engine) as api:
        engine.fs = api.fs
        workflow_id = create_workflow(api, source=TEMPLATE_DIR)
        user_id = create_user(api)
        group_id = create_group(api, workflow_id, [user_id])
        names = FakeStream(data=['Alice', 'Bob', 'Zoe'], format='plain/text')
        file_id = upload_file(api, group_id, user_id, names.save())
        args = [
            ARG('names', FILE(file_id, target)),
            ARG('sleeptime', 1),
            ARG('greeting', 'Hi')
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
    fh, filename = api.runs().get_result_file(
        run_id=run_id,
        file_id=files['results/greetings.txt'],
        user_id=user_id
    )
    greetings = util.read_text(file=filename)
    assert 'Hi Alice' in greetings
    assert 'Hi Bob' in greetings
    assert 'Hi Zoe' in greetings
    fh, filename = api.runs().get_result_file(
        run_id=run_id,
        file_id=files['results/analytics.json'],
        user_id=user_id
    )
    assert read_json(filename) is not None
    # -- Clean-up environment variables ---------------------------------------
    del os.environ[FLOWSERV_DB]
    del os.environ[FLOWSERV_API_BASEDIR]
    del os.environ[FLOWSERV_FILESTORE_MODULE]
    del os.environ[FLOWSERV_FILESTORE_CLASS]
