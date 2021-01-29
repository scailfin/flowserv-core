# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for canceling workflow runs."""

import pytest

from flowserv.model.workflow.state import STATE_PENDING
from flowserv.tests.service import create_group, create_user, start_hello_world


import flowserv.error as err


def test_cancel_runs_local(local_service, hello_world):
    """Test canceling runs using a local service."""
    # -- Setup ----------------------------------------------------------------
    #
    # Start two runs for the same group of the 'Hello World' workflow.
    with local_service() as api:
        user_1 = create_user(api)
        user_2 = create_user(api)
        workflow_id = hello_world(api).workflow_id
    with local_service(user_id=user_1) as api:
        group_id = create_group(api, workflow_id=workflow_id)
        run_1, _ = start_hello_world(api, group_id)
        run_2, _ = start_hello_world(api, group_id)
        # -- Ensure that there are two active runs
        r = api.runs().list_runs(group_id=group_id)
        assert len(r['runs']) == 2
    # -- Ensure that user 2 cannot cancel run 2 -------------------------------
    with local_service(user_id=user_2) as api:
        with pytest.raises(err.UnauthorizedAccessError):
            api.runs().cancel_run(run_id=run_1)
    with local_service(user_id=user_1) as api:
        # There are still two active runs.
        r = api.runs().list_runs(group_id=group_id, state=STATE_PENDING)
        assert len(r['runs']) == 2
    # -- Cancel run 2 ---------------------------------------------------------
    with local_service(user_id=user_1) as api:
        api.runs().cancel_run(run_id=run_2)
        r = api.runs().list_runs(group_id=group_id, state=STATE_PENDING)
        runs = r['runs']
        assert len(runs) == 1
        assert run_1 in [r['id'] for r in runs]
    # -- Error when canceling an inactive run ---------------------------------
    with local_service(user_id=user_1) as api:
        with pytest.raises(err.InvalidRunStateError):
            api.runs().cancel_run(run_id=run_2)


def test_cancel_run_remote(remote_service, mock_response):
    """Test cancelling a workflow run at the remote service."""
    remote_service.runs().cancel_run(run_id='0000')
    remote_service.runs().cancel_run(run_id='0000', reason='Test')
