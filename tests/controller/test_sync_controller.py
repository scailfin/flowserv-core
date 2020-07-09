# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the synchronous mode of the serial workflow controller."""

import os
import pytest

from flowserv.controller.serial.engine import SerialWorkflowEngine
from flowserv.service.run import ARG_ID, ARG_VALUE, ARG_AS
from flowserv.tests.files import FakeStream
from flowserv.tests.service import (
    create_group, create_user, create_workflow, start_run, upload_file
)

import flowserv.util as util
import flowserv.model.workflow.state as st
import flowserv.tests.serialize as serialize


DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../.files/template')
# Workflow templates
TEMPLATE_HELLOWORLD = os.path.join(TEMPLATE_DIR, './hello-world.yaml')
INVALID_TEMPLATE = './template-invalid-cmd.yaml'
TEMPLATE_WITH_INVALID_CMD = os.path.join(TEMPLATE_DIR, INVALID_TEMPLATE)


@pytest.mark.parametrize(
    'specfile,state',
    [
        (TEMPLATE_HELLOWORLD, st.STATE_SUCCESS),
        (TEMPLATE_WITH_INVALID_CMD, st.STATE_ERROR)
    ]
)
def test_run_helloworld_sync(api_factory, specfile, state):
    """Execute the helloworld example."""
    api = api_factory(engine=SerialWorkflowEngine(is_async=False))
    # Start a new run for the workflow template.
    workflow_id = create_workflow(
        api,
        sourcedir=TEMPLATE_DIR,
        specfile=specfile
    )
    user_id = create_user(api)
    group_id = create_group(api, workflow_id, [user_id])
    names = FakeStream(data=['Alice', 'Bob'], format='plain/text')
    file_id = upload_file(api, group_id, user_id, names)
    run_id = start_run(
        api,
        group_id,
        user_id,
        arguments=[
            {ARG_ID: 'names', ARG_VALUE: file_id, ARG_AS: 'data/names.txt'},
            {ARG_ID: 'sleeptime', ARG_VALUE: 3}
        ]
    )
    # Validate the run handle against the expected state.
    doc = api.runs().get_run(run_id, user_id)
    serialize.validate_run_handle(doc, state=state)
    if state == st.STATE_SUCCESS:
        # The run should have the greetings.txt file as a result.
        files = dict()
        for obj in doc['files']:
            files[obj['name']] = obj['id']
        assert len(files) == 1
        fh = api.runs().get_result_file(
            run_id=run_id,
            file_id=files['results/greetings.txt'],
            user_id=user_id
        )
        assert util.read_object(fh.filename) == 'Hello Alice! Hello Bob!'
