# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for executing serial workflow steps in a Docker container
environment.
"""

import os

from flowserv.controller.worker.docker import DockerWorker
from flowserv.model.workflow.step import ContainerStep


# Test files directory
DIR = os.path.dirname(os.path.realpath(__file__))
RUN_DIR = os.path.join(DIR, '../../.files')


def test_run_steps_with_error(mock_docker):
    """Test execution of a workflow step where one of the commands raises an
    error.
    """
    # Run with exception raised.
    commands = [
        'TEST_ENV_1',
        'error',
        'TEST_ENV_2'
    ]
    env = {'TEST_ENV_1': ('Hello', 0), 'TEST_ENV_2': ('World', 0)}
    step = ContainerStep(identifier='test', image='test', commands=commands)
    result = DockerWorker().run(step=step, env=env, rundir=RUN_DIR)
    assert result.returncode == 1
    assert result.exception is not None
    assert result.stdout == ['Hello']
    assert 'there was an error' in ''.join(result.stderr)
    # Run with command exit code being '1'.
    commands = [
        'TEST_ENV_1',
        'TEST_ENV_2'
    ]
    env = {'TEST_ENV_1': ('', 0), 'TEST_ENV_2': ('World', 1)}
    step = ContainerStep(identifier='test', image='test', commands=commands)
    result = DockerWorker().run(step=step, env=env, rundir=RUN_DIR)
    assert result.returncode == 1
    assert result.exception is None
    assert result.stdout == ['World']


def test_run_successful_steps(mock_docker):
    """Test successful execution of a workflow step with two commands."""
    commands = [
        'TEST_ENV_1',
        'TEST_ENV_2'
    ]
    env = {'TEST_ENV_1': ('Hello', 0), 'TEST_ENV_2': ('World', 0)}
    step = ContainerStep(identifier='test', image='test', commands=commands)
    result = DockerWorker().run(step=step, env=env, rundir=RUN_DIR)
    assert result.returncode == 0
    assert result.exception is None
    assert result.stdout == ['Hello', 'World']
    assert result.stderr == []
