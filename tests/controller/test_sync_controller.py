# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the synchronous mode of the serial workflow controller."""

from contextlib import contextmanager


import os
import pytest

from flowserv.config import Config
from flowserv.controller.serial.engine import SerialWorkflowEngine
from flowserv.model.database import DB, TEST_URL
from flowserv.model.files.fs import FileSystemStore
from flowserv.service.local import create_local_api
from flowserv.service.run.argument import serialize_arg, serialize_fh
from flowserv.tests.controller import StateEngine
from flowserv.tests.files import io_file
from flowserv.tests.service import (
    create_group, create_user, create_workflow, start_run, upload_file
)


import flowserv.model.workflow.state as st
import flowserv.tests.serialize as serialize


DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../.files/template')
# Workflow templates
TEMPLATE_HELLOWORLD = os.path.join(TEMPLATE_DIR, './hello-world.yaml')
INVALID_TEMPLATE = './template-invalid-cmd.yaml'
TEMPLATE_WITH_INVALID_CMD = os.path.join(TEMPLATE_DIR, INVALID_TEMPLATE)


@pytest.fixture
def database():
    """Create a fresh instance of the database."""
    db = DB(connect_url=TEST_URL, web_app=False)
    db.init()
    return db


@pytest.fixture
def local_service(database):
    """Factory pattern for service API objects."""
    @contextmanager
    def _api(config, engine=StateEngine(), auth=None, user_id=None, access_token=None):
        with database.session() as session:
            yield create_local_api(
                session=session,
                engine=engine,
                fs=FileSystemStore(config=config),
                config=config,
                auth=auth,
                user_id=user_id,
                access_token=access_token
            )

    return _api


@pytest.mark.parametrize(
    'specfile,state',
    [
        (TEMPLATE_HELLOWORLD, st.STATE_SUCCESS),
        (TEMPLATE_WITH_INVALID_CMD, st.STATE_ERROR)
    ]
)
def test_run_helloworld_sync(local_service, specfile, state, tmpdir):
    """Execute the helloworld example."""
    # -- Setup ----------------------------------------------------------------
    #
    config = Config().basedir(tmpdir)
    # Start a new run for the workflow template.
    engine = SerialWorkflowEngine(config=config)
    with local_service(config=config, engine=engine) as api:
        workflow_id = create_workflow(
            api,
            source=TEMPLATE_DIR,
            specfile=specfile
        )
        user_id = create_user(api)
    with local_service(config=config, engine=engine, user_id=user_id) as api:
        group_id = create_group(api, workflow_id)
        names = io_file(data=['Alice', 'Bob'], format='plain/text')
        file_id = upload_file(api, group_id, names)
        args = [
            serialize_arg('names', serialize_fh(file_id, 'data/names.txt')),
            serialize_arg('sleeptime', 3)
        ]
        run_id = start_run(api, group_id, arguments=args)
    # -- Validate the run handle against the expected state -------------------
    with local_service(config=config, engine=engine, user_id=user_id) as api:
        r = api.runs().get_run(run_id)
        serialize.validate_run_handle(r, state=state)
        if state == st.STATE_SUCCESS:
            # The run should have the greetings.txt file as a result.
            files = dict()
            for obj in r['files']:
                files[obj['name']] = obj['id']
            assert len(files) == 1
            fh = api.runs().get_result_file(
                run_id=run_id,
                file_id=files['results/greetings.txt']
            )
            value = fh.read().decode('utf-8').strip()
            assert value == 'Hello Alice!\nHello Bob!'
