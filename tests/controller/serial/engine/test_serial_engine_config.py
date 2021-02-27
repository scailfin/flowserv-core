# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for overriding worker configuration in a serial workflow run."""

import docker
import os
import pytest

from flowserv.controller.worker.factory import Docker
from flowserv.tests.service import create_group, create_user, create_workflow, start_run

import flowserv.model.workflow.state as state


DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../../../.files/template')
TEMPLATE_HELLOWORLD = os.path.join(TEMPLATE_DIR, './test-workflow.yaml')


# -- Patch subprocess run -----------------------------------------------------

class MockClient:
    """Mock Docker client."""
    @property
    def containers(self):
        return self

    def run(self, image, command, volumes, remove, environment, stdout):
        """Mock run for docker container."""
        return command.encode('utf-8')


@pytest.fixture
def mock_docker(monkeypatch):
    """Raise error in subprocess.run()."""

    def mock_client(*args, **kwargs):
        return MockClient()

    monkeypatch.setattr(docker, "from_env", mock_client)


def test_override_worker_config(sync_service, mock_docker):
    """Execute workflow with modified worker configuration."""
    # Create workflow.
    with sync_service() as api:
        workflow_id = create_workflow(
            api,
            source=TEMPLATE_DIR,
            specfile=TEMPLATE_HELLOWORLD
        )
        user_id = create_user(api)
    # Start new run with default workers.
    with sync_service(user_id=user_id) as api:
        group_id = create_group(api, workflow_id)
        run_id = start_run(api, group_id, arguments=dict())
    with sync_service(user_id=user_id) as api:
        r = api.runs().get_run(run_id)
        assert r['state'] == state.STATE_ERROR
    # Start new run with modified workers.
    worker_config = {'workers': {'test': Docker()}}
    with sync_service(user_id=user_id) as api:
        group_id = create_group(api, workflow_id)
        run_id = start_run(api, group_id, arguments=dict(), config=worker_config)
    with sync_service(user_id=user_id) as api:
        r = api.runs().get_run(run_id)
        assert r['state'] == state.STATE_SUCCESS
