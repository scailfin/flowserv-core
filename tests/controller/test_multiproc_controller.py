# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the asynchronous multiprocess workflow controller."""

import json
import os
import pytest
import time

from flowserv.config import Config, FLOWSERV_FILESTORE_CLASS, FLOWSERV_FILESTORE_MODULE
from flowserv.model.database import TEST_DB
from flowserv.controller.serial.engine import SerialWorkflowEngine
from flowserv.service.run.argument import serialize_arg, serialize_fh
from flowserv.tests.files import io_file
from flowserv.tests.service import (
    create_group, create_user, create_service, create_workflow, start_run,
    upload_file
)


import flowserv.model.workflow.state as st


# Template directory
DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../.files/benchmark/helloworld')


def test_cancel_run_helloworld(tmpdir):
    """Test cancelling a helloworld run."""
    # -- Setup ----------------------------------------------------------------
    #
    config = Config().basedir(tmpdir).database(TEST_DB(tmpdir)).run_async()
    engine = SerialWorkflowEngine(config=config)
    service = create_service(engine=engine, config=config)
    # Start a new run for the workflow template.
    with service() as api:
        workflow_id = create_workflow(api, source=TEMPLATE_DIR)
        user_id = create_user(api)
    with service(user_id=user_id) as api:
        group_id = create_group(api, workflow_id)
        names = io_file(data=['Alice', 'Bob', 'Zoe'], format='plain/text')
        file_id = upload_file(api, group_id, names)
        args = [
            serialize_arg('names', serialize_fh(file_id)),
            serialize_arg('sleeptime', 10),
            serialize_arg('greeting', 'Hi')
        ]
        run_id = start_run(api, group_id, arguments=args, service=service)
    # Poll run after sleeping for one second.
    time.sleep(1)
    with service(user_id=user_id) as api:
        run = api.runs().get_run(run_id=run_id)
    assert run['state'] in st.ACTIVE_STATES
    # -- Cancel the active run ------------------------------------------------
    with service(user_id=user_id) as api:
        run = api.runs().cancel_run(
            run_id=run_id,
            reason='done'
        )
        assert run['state'] == st.STATE_CANCELED
        assert run['messages'][0] == 'done'
    with service(user_id=user_id) as api:
        run = api.runs().get_run(run_id=run_id)
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
    config = Config().basedir(tmpdir).database(TEST_DB(tmpdir)).run_async()
    config.update(fsconfig)
    engine = SerialWorkflowEngine(config=config)
    service = create_service(engine=engine, config=config)
    # Start a new run for the workflow template.
    with service() as api:
        workflow_id = create_workflow(api, source=TEMPLATE_DIR)
        user_id = create_user(api)
    with service(user_id=user_id) as api:
        group_id = create_group(api, workflow_id)
        names = io_file(data=['Alice', 'Bob', 'Zoe'], format='plain/text')
        file_id = upload_file(api, group_id, names)
        args = [
            serialize_arg('names', serialize_fh(file_id, target)),
            serialize_arg('sleeptime', 1),
            serialize_arg('greeting', 'Hi')
        ]
        run_id = start_run(api, group_id, arguments=args, service=service)
    # Poll workflow state every second.
    with service(user_id=user_id) as api:
        run = api.runs().get_run(run_id=run_id)
    while run['state'] in st.ACTIVE_STATES:
        time.sleep(1)
        with service(user_id=user_id) as api:
            run = api.runs().get_run(run_id=run_id)
    assert run['state'] == st.STATE_SUCCESS
    files = dict()
    for f in run['files']:
        files[f['name']] = f['id']
    fh = api.runs().get_result_file(
        run_id=run_id,
        file_id=files['results/greetings.txt']
    )
    greetings = fh.read().decode('utf-8').strip()
    assert 'Hi Alice' in greetings
    assert 'Hi Bob' in greetings
    assert 'Hi Zoe' in greetings
    fh = api.runs().get_result_file(
        run_id=run_id,
        file_id=files['results/analytics.json']
    )
    assert json.load(fh) is not None
