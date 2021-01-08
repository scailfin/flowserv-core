# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for deleting workflow runs."""

import pytest

from flowserv.config import Config
from flowserv.model.workflow.state import STATE_PENDING
from flowserv.tests.service import create_group, create_user, start_hello_world


import flowserv.error as err


def test_delete_runs_local(local_service, hello_world, tmpdir):
    """Test deleting runs."""
    # -- Setup ----------------------------------------------------------------
    #
    config = Config().basedir(tmpdir)
    # Start two runs for a group of the 'Hello World' workflow. The first run
    # is active and the second run canceled.
    with local_service(config=config) as api:
        user_1 = create_user(api)
        user_2 = create_user(api)
        workflow_id = hello_world(api).workflow_id
    with local_service(config=config, user_id=user_1) as api:
        group_id = create_group(api, workflow_id=workflow_id)
        run_1, _ = start_hello_world(api, group_id)
        run_2, _ = start_hello_world(api, group_id)
        api.runs().cancel_run(run_id=run_2)
    # -- Ensure that we cannot delete an active run ---------------------------
    with local_service(config=config, user_id=user_1) as api:
        with pytest.raises(err.InvalidRunStateError):
            api.runs().delete_run(run_id=run_1)
        r = api.runs().list_runs(group_id=group_id, state=STATE_PENDING)
        assert len(r['runs']) == 1
    # -- Ensure that user 2 cannot delete run 1 -------------------------------
    with local_service(config=config, user_id=user_2) as api:
        with pytest.raises(err.UnauthorizedAccessError):
            api.runs().delete_run(run_id=run_2)
    # -- Delete run 1 ---------------------------------------------------------
    with local_service(config=config, user_id=user_1) as api:
        api.runs().delete_run(run_id=run_2)
        r = api.runs().list_runs(group_id=group_id)
        # The active run is not affected.
        assert len(r['runs']) == 1
    # -- Error when deleting an unknown run -----------------------------------
    with local_service(config=config, user_id=user_1) as api:
        with pytest.raises(err.UnknownRunError):
            api.runs().delete_run(run_id=run_2)


def test_delete_run_remote(remote_service, mock_response):
    """Test deleting a workflow run at the remote service."""
    remote_service.runs().delete_run(run_id='0000')
