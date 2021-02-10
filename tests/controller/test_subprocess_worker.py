# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for executing serial workflow steps in a subprocess environment."""

import os
import pytest
import subprocess

from flowserv.controller.serial.subprocess import SubprocessWorker
from flowserv.controller.serial.workflow import ContainerStep


# Template directory
DIR = os.path.dirname(os.path.realpath(__file__))
RUN_DIR = os.path.join(DIR, '../.files')


# -- Patching for error condition testing -------------------------------------

@pytest.fixture
def mock_subprocess(monkeypatch):
    """Raise error in subprocess.run()."""

    def mock_run(*args, **kwargs):
        raise ValueError('cannot run')

    monkeypatch.setattr(subprocess, "run", mock_run)


# -- Unit tests ---------------------------------------------------------------

def test_run_steps_with_error():
    """Test execution of a workflow step where one of the commands raises an
    error.
    """
    commands = [
        'python printenv.py TEST_ENV_1',
        'python printenv.py TEST_ENV_ERROR',
        'python printenv.py TEST_ENV_2'
    ]
    env = {'TEST_ENV_1': 'Hello', 'TEST_ENV_ERROR': 'error', 'TEST_ENV_2': 'World'}
    step = ContainerStep(image='test', commands=commands, env=env)
    result = SubprocessWorker().run(step=step, rundir=RUN_DIR)
    assert result.returncode == 1
    assert result.exception is None
    assert result.stdout == ['Hello\n']
    assert 'there was an error' in ''.join(result.stderr)


def test_run_steps_with_subprocess_error(mock_subprocess):
    """Test execution of a workflow step that fails to run."""
    commands = ['nothing to do']
    step = ContainerStep(image='test', commands=commands)
    result = SubprocessWorker().run(step=step, rundir=RUN_DIR)
    assert result.returncode == 1
    assert result.exception is not None
    assert result.stdout == []
    assert 'cannot run' in ''.join(result.stderr)


def test_run_successful_steps():
    """Test successful execution of a workflow step with two commands."""
    commands = [
        'python printenv.py TEST_ENV_1',
        'python printenv.py TEST_ENV_2'
    ]
    env = {'TEST_ENV_1': 'Hello', 'TEST_ENV_2': 'World'}
    step = ContainerStep(image='test', commands=commands, env=env)
    result = SubprocessWorker().run(step=step, rundir=RUN_DIR)
    assert result.returncode == 0
    assert result.exception is None
    assert result.stdout == ['Hello\n', 'World\n']
    assert result.stderr == []
