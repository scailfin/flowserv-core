# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for the remote workflow monitor."""

from flowserv.controller.remote.monitor import monitor_workflow
from flowserv.controller.remote.client import RemoteWorkflowHandle
from flowserv.model.workflow.state import StatePending
from flowserv.tests.remote import RemoteTestClient
from flowserv.volume.fs import FileSystemStorage


def test_remote_run_error(tmpdir):
    """Test monitoring an erroneous workflow run."""
    # Create client that will raise an error after the default rounds of polling.
    client = RemoteTestClient(error='some error')
    workflow = RemoteWorkflowHandle(
        run_id='R0',
        workflow_id='W0',
        state=StatePending(),
        output_files=list(),
        runstore=FileSystemStorage(basedir=tmpdir),
        client=client
    )
    state = monitor_workflow(workflow=workflow, poll_interval=0.1)
    assert state.is_error()


def test_remote_run_success(tmpdir):
    """Test monitoring a successful workflow run."""
    client = RemoteTestClient()
    workflow = RemoteWorkflowHandle(
        run_id='R0',
        workflow_id='W0',
        state=StatePending(),
        output_files=list(),
        runstore=FileSystemStorage(basedir=tmpdir),
        client=client
    )
    state = monitor_workflow(workflow=workflow, poll_interval=0.1)
    assert state.is_success()
