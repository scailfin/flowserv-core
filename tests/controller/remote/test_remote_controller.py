# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the generic remote workflow engine controller."""

import os
import pytest
import time

from flowserv.config import Config
from flowserv.controller.remote.engine import RemoteWorkflowController
from flowserv.tests.remote import RemoteTestClient
from flowserv.tests.service import (
    create_group, create_user, create_workflow, start_run
)
from flowserv.service.local import LocalAPIFactory
from flowserv.volume.manager import FStore

import flowserv.model.workflow.state as st
import flowserv.tests.serialize as serialize


# Template directory
DIR = os.path.dirname(os.path.realpath(__file__))
BENCHMARK_DIR = os.path.join(DIR, '..', '..', '.files', 'benchmark', 'remote')


def test_cancel_remote_workflow(tmpdir):
    """Cancel the execution of a remote workflow."""
    # -- Setup ----------------------------------------------------------------
    env = Config().volume(FStore(basedir=str(tmpdir))).auth()
    engine = RemoteWorkflowController(
        client=RemoteTestClient(runcount=100),
        poll_interval=0.25,
        is_async=True
    )
    service = LocalAPIFactory(env=env, engine=engine)
    # Need to set the association between the engine and the service explicitly
    # after the API is created.
    engine.service = service
    with service() as api:
        workflow_id = create_workflow(api, source=BENCHMARK_DIR)
        user_id = create_user(api)
    with service(user_id=user_id) as api:
        group_id = create_group(api, workflow_id)
    # -- Unit test ------------------------------------------------------------
    # Start a new run
    with service(user_id=user_id) as api:
        run_id = start_run(api, group_id)
    # -- Poll workflow state every second.
    with service(user_id=user_id) as api:
        run = api.runs().get_run(run_id=run_id)
    watch_dog = 30
    while run['state'] == st.STATE_PENDING and watch_dog:
        time.sleep(0.1)
        watch_dog -= 1
        with service(user_id=user_id) as api:
            run = api.runs().get_run(run_id=run_id)
    serialize.validate_run_handle(run, state=st.STATE_RUNNING)
    with service(user_id=user_id) as api:
        api.runs().cancel_run(run_id=run_id, reason='test')
    # Sleep to ensure that the workflow monitor polls the state and makes an
    # attempt to update the run state. This should raise an error for the
    # monitor. The error is not propagated here or to the run.
    time.sleep(1)
    with service(user_id=user_id) as api:
        run = api.runs().get_run(run_id=run_id)
    serialize.validate_run_handle(run, state=st.STATE_CANCELED)
    assert run['messages'][0] == 'test'


@pytest.mark.parametrize('is_async', [False, True])
def test_run_remote_workflow_error(is_async, tmpdir):
    """Execute the remote workflow example synchronized and in asynchronous
    mode when execution results in an error state.
    """
    # -- Setup ----------------------------------------------------------------
    env = Config().volume(FStore(basedir=str(tmpdir))).auth()
    engine = RemoteWorkflowController(
        client=RemoteTestClient(runcount=3, error='some error'),
        poll_interval=0.1,
        is_async=is_async
    )
    service = LocalAPIFactory(env=env, engine=engine)
    # Need to set the association between the engine and the service explicitly
    # after the API is created.
    engine.service = service
    with service() as api:
        workflow_id = create_workflow(api, source=BENCHMARK_DIR)
        user_id = create_user(api)
    with service(user_id=user_id) as api:
        group_id = create_group(api, workflow_id)
    # -- Unit test ------------------------------------------------------------
    # Start a new run
    with service(user_id=user_id) as api:
        run_id = start_run(api, group_id)
    # Poll workflow state every second.
    with service(user_id=user_id) as api:
        run = api.runs().get_run(run_id=run_id)
    watch_dog = 30
    while run['state'] in st.ACTIVE_STATES and watch_dog:
        time.sleep(1)
        watch_dog -= 1
        with service(user_id=user_id) as api:
            run = api.runs().get_run(run_id=run_id)
    serialize.validate_run_handle(run, state=st.STATE_ERROR)
    assert run['messages'] == ['some error']


@pytest.mark.parametrize('is_async', [False, True])
def test_run_remote_workflow_success(is_async, tmpdir):
    """Successfully execute the remote workflow example synchronized and in
    asynchronous mode.
    """
    # -- Setup ----------------------------------------------------------------
    env = Config().volume(FStore(basedir=str(tmpdir))).auth()
    engine = RemoteWorkflowController(
        client=RemoteTestClient(runcount=3),
        poll_interval=0.1,
        is_async=is_async
    )
    service = LocalAPIFactory(env=env, engine=engine)
    # Need to set the association between the engine and the service explicitly
    # after the API is created.
    engine.service = service
    with service() as api:
        workflow_id = create_workflow(api, source=BENCHMARK_DIR)
        user_id = create_user(api)
    with service(user_id=user_id) as api:
        group_id = create_group(api, workflow_id)
    # -- Unit test ------------------------------------------------------------
    # Start a new run
    with service(user_id=user_id) as api:
        run_id = start_run(api, group_id)
    # Poll workflow state every second.
    with service(user_id=user_id) as api:
        run = api.runs().get_run(run_id=run_id)
    watch_dog = 30
    while run['state'] in st.ACTIVE_STATES and watch_dog:
        time.sleep(1)
        watch_dog -= 1
        with service(user_id=user_id) as api:
            run = api.runs().get_run(run_id=run_id)
    serialize.validate_run_handle(run, state=st.STATE_SUCCESS)
