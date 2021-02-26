# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the serial workflow run result class."""

from flowserv.controller.serial.workflow.result import ExecResult, RunResult
from flowserv.model.workflow.step import ContainerStep


def test_empty_run_result():
    """Test run result properties for a run without any executed workflow steps."""
    r = RunResult(arguments={'a': 1})
    assert len(r) == 0
    assert r.get('a') == 1
    assert r.exception is None
    assert r.returncode is None
    assert r.stderr == []
    assert r.stdout == []


def test_error_run_result():
    """Test results of an erroneous workflow run."""
    r = RunResult(arguments={})
    r.add(ExecResult(step=ContainerStep(image='test'), returncode=0))
    assert r.exception is None
    assert r.returncode == 0
    r.add(ExecResult(step=ContainerStep(image='test'), returncode=1, stderr=['e1', 'e2'], exception=ValueError()))
    assert r.exception is not None
    assert r.returncode == 1
    assert r.stdout == []
    assert r.stderr == ['e1', 'e2']


def test_successful_run_result():
    """Test results of a successful workflow run."""
    r = RunResult(arguments={'a': 1})
    r.add(ExecResult(step=ContainerStep(image='test1'), returncode=0, stdout=['o1']))
    r.context['a'] = 2
    r.add(ExecResult(step=ContainerStep(image='test2'), returncode=0, stdout=['o2', 'o3']))
    r.context['b'] = 1
    assert r.exception is None
    assert r.returncode == 0
    assert r.stdout == ['o1', 'o2', 'o3']
    assert r.stderr == []
    assert r.get('a') == 2
    assert r.get('b') == 1
    result = r.steps[0]
    assert result.step.image == 'test1'
    assert result.stdout == ['o1']
    result = r.steps[1]
    assert result.step.image == 'test2'
    assert result.stdout == ['o2', 'o3']
