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
from flowserv.service.run.argument import ARG, FILE
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
def test_run_helloworld_sync(service, specfile, state):
    """Execute the helloworld example."""
    # -- Setup ----------------------------------------------------------------
    #
    # Start a new run for the workflow template.
    engine = SerialWorkflowEngine(is_async=False)
    with service(engine=engine) as api:
        workflow_id = create_workflow(
            api,
            source=TEMPLATE_DIR,
            specfile=specfile
        )
        user_id = create_user(api)
        group_id = create_group(api, workflow_id, [user_id])
        names = FakeStream(data=['Alice', 'Bob'], format='plain/text')
        file_id = upload_file(api, group_id, user_id, names)
        args = [
            ARG('names', FILE(file_id, 'data/names.txt')),
            ARG('sleeptime', 3)
        ]
        run_id = start_run(api, group_id, user_id, arguments=args)
    # -- Validate the run handle against the expected state -------------------
    with service(engine=engine) as api:
        r = api.runs().get_run(run_id, user_id)
        serialize.validate_run_handle(r, state=state)
        if state == st.STATE_SUCCESS:
            # The run should have the greetings.txt file as a result.
            files = dict()
            for obj in r['files']:
                files[obj['name']] = obj['id']
            assert len(files) == 1
            fh = api.runs().get_result_file(
                run_id=run_id,
                file_id=files['results/greetings.txt'],
                user_id=user_id
            )
            assert util.read_object(fh.filename) == 'Hello Alice! Hello Bob!'
