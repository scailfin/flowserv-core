# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for creating workflow runs."""

import os
import pytest
import tempfile

from flowserv.config import Config
from flowserv.service.run.argument import is_fh
from flowserv.tests.service import (
    create_group, create_user, start_hello_world, write_results
)

import flowserv.error as err
import flowserv.model.workflow.state as st
import flowserv.tests.serialize as serialize


def test_create_run_local(local_service, hello_world, tmpdir):
    """Test life cycle for successful run using the local service."""
    # -- Setup ----------------------------------------------------------------
    #
    config = Config().basedir(tmpdir)
    # Start a new run for a group of the 'Hello World' workflow and set it into
    # success state.
    tmpdir = tempfile.mkdtemp()
    with local_service(config=config) as api:
        user_1 = create_user(api)
        user_2 = create_user(api)
        workflow_id = hello_world(api).workflow_id
    with local_service(config=config, user_id=user_1) as api:
        group_id = create_group(api, workflow_id=workflow_id, users=[user_1])
        run_id, file_id = start_hello_world(api, group_id)
        result = {'group': group_id, 'run': run_id}
        write_results(
            rundir=tmpdir,
            files=[
                (result, None, 'results/data.json'),
                ([group_id, run_id], 'txt/plain', 'values.txt')
            ]
        )
        api.runs().update_run(
            run_id=run_id,
            state=api.runs().backend.success(
                run_id,
                files=['results/data.json', 'values.txt']
            ),
            rundir=tmpdir
        )
    assert not os.path.exists(tmpdir)
    # -- Validate run handle --------------------------------------------------
    with local_service(config=config, user_id=user_1) as api:
        r = api.runs().get_run(run_id=run_id)
        serialize.validate_run_handle(r, st.STATE_SUCCESS)
        assert is_fh(r['arguments'][0]['value'])
    # -- Error when non-member attempts to access run -------------------------
    with local_service(config=config, user_id=user_2) as api:
        with pytest.raises(err.UnauthorizedAccessError):
            api.runs().get_run(run_id=run_id)


def test_start_run_remote(remote_service, mock_response):
    """Test starting a workflow run at the remote service."""
    remote_service.runs().start_run(group_id='0000', arguments=[{'arg': 1}])
    remote_service.runs().get_run(run_id='0000')
