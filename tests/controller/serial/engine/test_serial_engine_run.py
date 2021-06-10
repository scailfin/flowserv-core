# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for running a sequence of worklfow steps."""

from collections import namedtuple

import json
import os
import pytest
import subprocess

from flowserv.controller.serial.engine.base import run_workflow
from flowserv.controller.serial.workflow.base import SerialWorkflow
from flowserv.model.workflow.state import StatePending, STATE_ERROR
from flowserv.volume.manager import DefaultVolume


# -- Patch subprocess run -----------------------------------------------------

Proc = namedtuple('Proc', ['returncode', 'stdout', 'stderr'])


@pytest.fixture
def mock_subprocess(monkeypatch):
    """Raise error in subprocess.run()."""

    def mock_run(*args, **kwargs):
        val = args[0].split()[1]
        if val == 'error':
            raise ValueError('cannot run')
        else:
            with open(os.path.join(kwargs['cwd'], 'test_result.json'), 'w') as f:
                json.dump({'result': val}, f)
            return Proc(returncode=0, stdout=b'', stderr=b'')
    monkeypatch.setattr(subprocess, "run", mock_run)


def myfunc(a):
    print('input {}'.format(a))
    return 'error' if a == 0 else 'go'


@pytest.fixture
def workflow():
    return SerialWorkflow()\
        .add_code_step(identifier='s1', func=myfunc, arg='b')\
        .add_container_step(identifier='s2', image='test', commands=['py $b'])


# -- Unit tests ---------------------------------------------------------------

def test_error_run(mock_subprocess, workflow, tmpdir):
    """Test error in workflow run."""
    r = workflow.run(arguments={'a': 0}, volumes=DefaultVolume(basedir=tmpdir))
    assert r.returncode == 1
    assert r.steps[1].step.commands == ['py error']
    assert r.stdout == ['input 0', '\n']
    assert r.stderr != []


def test_run_workflow_error(tmpdir):
    """Test error edge case for the run_workflow function."""
    # This will cause a NonType exception when trying to access the steps in
    # the exec_workflow method.
    _, _, result = run_workflow(
        run_id='0',
        state=StatePending(),
        output_files=[],
        steps=None,
        arguments=dict(),
        volumes=DefaultVolume(basedir=tmpdir),
        workers=None
    )
    assert result['type'] == STATE_ERROR


def test_successful_run(mock_subprocess, workflow, tmpdir):
    """Test successful workflow run."""
    r = workflow.run(arguments={'a': 1}, volumes=DefaultVolume(basedir=tmpdir))
    with open(os.path.join(tmpdir, 'test_result.json'), 'r') as f:
        doc = json.load(f)
    # assert r.returncode == 0
    assert r.steps[1].step.commands == ['py go']
    assert r.stdout == ['input 1', '\n']
    assert r.stderr == []
    assert doc['result'] == 'go'
