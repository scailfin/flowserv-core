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


def test_delete_runs(api_factory, hello_world):
    """Test deleting runs."""
    # Initialize the database and the API.
    api = api_factory()
    user_1 = create_user(api)
    user_2 = create_user(api)
    workflow_id = hello_world(api)['id']
    group_id = create_group(api, workflow_id=workflow_id, users=[user_1])
    # Start two new runs.
    run_1, _ = start_hello_world(api, group_id, user_1)
    run_2, _ = start_hello_world(api, group_id, user_1)
    doc = api.runs().poll_runs(group_id=group_id, user_id=user_1)
    assert len(doc['runs']) == 2
    # Ensure that we cannot delete an active run.
    with pytest.raises(err.InvalidRunStateError):
        api.runs().delete_run(run_id=run_1, user_id=user_1)
    doc = api.runs().poll_runs(group_id=group_id, user_id=user_1)
    assert len(doc['runs']) == 2
    # Cancel run 2.
    api.runs().cancel_run(run_id=run_2, user_id=user_1)
    # Ensure that user 2 cannot delete the run.
    with pytest.raises(err.UnauthorizedAccessError):
        api.runs().delete_run(run_id=run_2, user_id=user_2)
        # user 1 can delete run 2.
    api.runs().delete_run(run_id=run_2, user_id=user_1)
    doc = api.runs().poll_runs(group_id=group_id, user_id=user_1)
    runs = doc['runs']
    assert len(runs) == 1
    assert run_1 in runs
    # Error when deleting an unknown run.
    with pytest.raises(err.UnknownRunError):
        api.runs().delete_run(run_id=run_2, user_id=user_1)
