# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for creating workflow runs."""

import pytest

from flowserv.tests.service import (
    create_group, create_user, start_hello_world, write_results
)

import flowserv.error as err
import flowserv.model.workflow.state as st
import flowserv.tests.serialize as serialize


def test_create_successful_run(api_factory, hello_world):
    """Test life cycle for successful run."""
    # Initialize the database and the API.
    api = api_factory()
    user_1 = create_user(api)
    user_2 = create_user(api)
    workflow_id = hello_world(api)['id']
    group_id = create_group(api, workflow_id=workflow_id, users=[user_1])
    # Start the new run.
    run_id, file_id = start_hello_world(api, group_id, user_1)
    # Validate handle for active run.
    rh = api.runs().get_run(run_id=run_id, user_id=user_1)
    serialize.validate_run_handle(rh, st.STATE_PENDING)
    # Write run result files.
    write_results(
        api,
        run_id,
        [
            ({'group': group_id, 'run': run_id}, None, 'results/data.json'),
            ([group_id, run_id], 'txt/plain', 'values.txt')
        ]
    )
    api.runs().update_run(
        run_id=run_id,
        state=api.engine.success(
            run_id,
            files=['results/data.json', 'values.txt']
        )
    )
    # Get the run handle.
    rh = api.runs().get_run(run_id=run_id, user_id=user_1)
    serialize.validate_run_handle(rh, st.STATE_SUCCESS)
    # The input argument should be a serialized input file with file id and
    # name.
    file_arg = rh['arguments'][0]['value']['file']
    assert file_arg['id'] is not None
    assert file_arg['name'] == 'n.txt'
    # Error when non-member attempts to access run.
    with pytest.raises(err.UnauthorizedAccessError):
        api.runs().get_run(run_id=run_id, user_id=user_2)
