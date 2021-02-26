# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for executing workflow code steps."""

import os

from flowserv.controller.worker.code import exec_func
from flowserv.model.workflow.step import FunctionStep


def write_and_add(a):
    with open('out.txt', 'w') as f:
        f.write('{}'.format(a))
        print('{} written'.format(a))
    if a < 0:
        raise ValueError('invalid value {}'.format(a))
    return a + 1


def test_error_exec(tmpdir):
    """Test error when running a code step."""
    step = FunctionStep(func=write_and_add, output='a')
    r = exec_func(step=step, context={'a': -1}, rundir=tmpdir)
    assert r.returncode == 1
    assert r.stdout == ['-1 written', '\n']
    assert r.stderr != []
    assert r.exception is not None


def test_successful_exec(tmpdir):
    """Test successfully running a code step."""
    step = FunctionStep(func=write_and_add, output='a')
    r = exec_func(step=step, context={'a': 1}, rundir=tmpdir)
    assert r.returncode == 0
    assert r.stdout == ['1 written', '\n']
    assert r.stderr == []
    assert r.exception is None
    # Read the written output file.
    with open(os.path.join(tmpdir, 'out.txt'), 'r') as f:
        for line in f:
            line = line.strip()
    assert line == '1'
