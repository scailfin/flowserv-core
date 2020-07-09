# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for deleting workflow runs."""

import pytest

from flowserv.tests.service import create_group, create_user, start_hello_world


import flowserv.error as err


def test_delete_runs_view(service, hello_world):
    """Test deleting runs."""
    # -- Setup ----------------------------------------------------------------
    #
    # Start two runs for a group of the 'Hello World' workflow. The first run
    # is active and the second run canceled.
    with service() as api:
        user_1 = create_user(api)
        user_2 = create_user(api)
        workflow_id = hello_world(api)['id']
        group_id = create_group(api, workflow_id=workflow_id, users=[user_1])
        run_1, _ = start_hello_world(api, group_id, user_1)
        run_2, _ = start_hello_world(api, group_id, user_1)
        api.runs().cancel_run(run_id=run_2, user_id=user_1)
    # -- Ensure that we cannot delete an active run ---------------------------
    with service() as api:
        with pytest.raises(err.InvalidRunStateError):
            api.runs().delete_run(run_id=run_1, user_id=user_1)
        r = api.runs().poll_runs(group_id=group_id, user_id=user_1)
        assert len(r['runs']) == 1
    # -- Ensure that user 2 cannot delete run 1 -------------------------------
    with service() as api:
        with pytest.raises(err.UnauthorizedAccessError):
            api.runs().delete_run(run_id=run_2, user_id=user_2)
    # -- Delete run 1 ---------------------------------------------------------
    with service() as api:
        api.runs().delete_run(run_id=run_2, user_id=user_1)
        r = api.runs().poll_runs(group_id=group_id, user_id=user_1)
        runs = r['runs']
        # The active run is not affected.
        assert len(runs) == 1
    # -- Error when deleting an unknown run -----------------------------------
    with service() as api:
        with pytest.raises(err.UnknownRunError):
            api.runs().delete_run(run_id=run_2, user_id=user_1)
