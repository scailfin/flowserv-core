# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for executing a benchmark with a notebook step."""

import os

from flowserv.service.run.argument import serialize_arg
from flowserv.tests.service import create_group, create_user, create_workflow, start_run


import flowserv.model.workflow.state as st
import flowserv.tests.serialize as serialize


DIR = os.path.dirname(os.path.realpath(__file__))
BENCHMARK_DIR = os.path.join(DIR, '..', '..', '..', '.files', 'benchmark', 'helloworld')
SPEC_FILE = os.path.join(BENCHMARK_DIR, 'benchmark-with-notebook.yaml')


def test_run_helloworld_sync_docker(sync_service):
    """Successfully execute the helloworld example that contains a notebook step
    using a Docker container."""
    # -- Setup ----------------------------------------------------------------
    #
    # Start a new run for the workflow template.
    with sync_service() as api:
        workflow_id = create_workflow(
            api,
            source=BENCHMARK_DIR,
            specfile=SPEC_FILE
        )
        user_id = create_user(api)
    with sync_service(user_id=user_id) as api:
        group_id = create_group(api, workflow_id)
        args = [serialize_arg('greeting', 'Hey there')]
        run_id = start_run(
            api,
            group_id,
            arguments=args,
            config={
                'workers': [{'name': 'nbdocker', 'type': 'nbdocker'}],
                'workflow': [{'step': 'say_hello', 'worker': 'nbdocker'}]
            }
        )
    # -- Validate the run handle against the expected state -------------------
    with sync_service(user_id=user_id) as api:
        r = api.runs().get_run(run_id)
        serialize.validate_run_handle(r, state=st.STATE_SUCCESS)
        # The run should have the greetings.txt file as a result.
        files = dict()
        for obj in r['files']:
            files[obj['name']] = obj['id']
        assert len(files) == 2
        fh = api.runs().get_result_file(
            run_id=run_id,
            file_id=files['results/greetings.txt']
        )
        value = fh.open().read().decode('utf-8').strip()
        assert 'Hey there Alice!' in value
        assert 'Hey there Bob!' in value


def test_run_helloworld_sync_env(sync_service):
    """Successfully execute the helloworld example that contains a notebook step
    in the Python environment that rund flowServ."""
    # -- Setup ----------------------------------------------------------------
    #
    # Start a new run for the workflow template.
    with sync_service() as api:
        workflow_id = create_workflow(
            api,
            source=BENCHMARK_DIR,
            specfile=SPEC_FILE
        )
        user_id = create_user(api)
    with sync_service(user_id=user_id) as api:
        group_id = create_group(api, workflow_id)
        args = [serialize_arg('greeting', 'Hey there')]
        run_id = start_run(api, group_id, arguments=args)
    # -- Validate the run handle against the expected state -------------------
    with sync_service(user_id=user_id) as api:
        r = api.runs().get_run(run_id)
        serialize.validate_run_handle(r, state=st.STATE_SUCCESS)
        # The run should have the greetings.txt file as a result.
        files = dict()
        for obj in r['files']:
            files[obj['name']] = obj['id']
        assert len(files) == 2
        fh = api.runs().get_result_file(
            run_id=run_id,
            file_id=files['results/greetings.txt']
        )
        value = fh.open().read().decode('utf-8').strip()
        assert 'Hey there Alice!' in value
        assert 'Hey there Bob!' in value
