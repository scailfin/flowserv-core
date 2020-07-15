# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for polling the state of workflow runs."""

import pytest

from flowserv.tests.service import create_group, create_user, start_hello_world


import flowserv.error as err


def test_poll_active_runs_view(service, hello_world):
    """Test polling the run state."""
    # -- Setup ----------------------------------------------------------------
    #
    # Start two runs for a group of the 'Hello World' workflow. The first run
    # is active and the second run is in error state.
    with service() as api:
        user_1 = create_user(api)
        user_2 = create_user(api)
        workflow_id = hello_world(api)['id']
        group_id = create_group(api, workflow_id=workflow_id, users=[user_1])
        run_1, _ = start_hello_world(api, group_id, user_1)
        run_2, _ = start_hello_world(api, group_id, user_1)
        # Set run 2 into error state.
        error_state = api.engine.error(run_2, ['some errors'])
        api.runs().update_run(run_2, error_state)
    # -- Poll returns one active run ------------------------------------------
    with service() as api:
        r = api.runs().poll_runs(group_id=group_id, user_id=user_1)
        runs = r['runs']
        assert len(runs) == 1
        assert run_1 in runs
    # -- Error when polling runs as a non-member ------------------------------
    with service() as api:
        with pytest.raises(err.UnauthorizedAccessError):
            api.runs().poll_runs(group_id=group_id, user_id=user_2)
