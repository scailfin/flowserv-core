# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for listing workflow runs."""

import pytest

from flowserv.tests.service import create_group, create_user, start_hello_world

import flowserv.error as err
import flowserv.model.workflow.state as st
import flowserv.tests.serialize as serialize


def test_list_runs_view(service, hello_world):
    """Test listing runs."""
    # -- Setup ----------------------------------------------------------------
    #
    # Start one run each for two separate groups of the 'Hello World' workflow.
    with service() as api:
        user_1 = create_user(api)
        user_2 = create_user(api)
        workflow_id = hello_world(api)['id']
        group_1 = create_group(api, workflow_id=workflow_id, users=[user_1])
        group_2 = create_group(api, workflow_id=workflow_id, users=[user_2])
        run_1, _ = start_hello_world(api, group_1, user_1)
        run_2, _ = start_hello_world(api, group_2, user_2)
    # Define expected run listing for each group.
    runs = [(group_1, run_1, user_1), (group_2, run_2, user_2)]
    for group_id, run_id, user_id in runs:
        # -- Get run listing for group ----------------------------------------
        with service() as api:
            r = api.runs().list_runs(group_id, user_id)
            serialize.validate_run_listing(r)
            assert len(r['runs']) == 1
            r['runs'][0]['id'] == run_id
    # Start additional runs for group 1. Then set the run into error state.
    with service() as api:
        run_3, _ = start_hello_world(api, group_1, user_1)
        error_state = api.engine.error(run_3, ['some errors'])
        api.runs().update_run(run_3, error_state)
    # -- Group 1 now has two runs, one in pending state and one in error state
    with service() as api:
        r = api.runs().list_runs(group_1, user_1)
        assert len(r['runs']) == 2
        runs = [(r['id'], r['state']) for r in r['runs']]
        assert (run_1, st.STATE_PENDING) in runs
        assert (run_3, st.STATE_ERROR) in runs
    # -- Group 2 remains unchnaged --------------------------------------------
    with service() as api:
        r = api.runs().list_runs(group_2, user_2)
        assert len(r['runs']) == 1
    # -- Error when listing runs for group as non-member ----------------------
    with service() as api:
        with pytest.raises(err.UnauthorizedAccessError):
            api.runs().list_runs(group_1, user_2)