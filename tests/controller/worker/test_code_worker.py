# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for executing workflow code steps."""

import os

from flowserv.controller.worker.code import CodeWorker, OutputStream
from flowserv.model.workflow.step import CodeStep
from flowserv.volume.fs import FileSystemStorage


def write_and_add(a):
    with open('out.txt', 'w') as f:
        f.write('{}'.format(a))
        print('{} written'.format(a))
    if a < 0:
        raise ValueError('invalid value {}'.format(a))
    return a + 1


def test_error_exec(tmpdir):
    """Test error when running a code step."""
    step = CodeStep(identifier='test', func=write_and_add, arg='a')
    r = CodeWorker().exec(step=step, context={'a': -1}, store=FileSystemStorage(tmpdir))
    assert r.returncode == 1
    assert r.stdout == ['-1 written', '\n']
    assert r.stderr != []
    assert r.exception is not None


def test_output_stream():
    """Test all methods of the OutputStream helper class."""
    result = list()
    stream = OutputStream(stream=result)
    stream.writelines(['A', 'B', 'C'])
    stream.write('D')
    stream.flush()
    stream.close()
    assert result == ['A', 'B', 'C', 'D']


def test_successful_exec(tmpdir):
    """Test successfully running a code step."""
    step = CodeStep(identifier='test', func=write_and_add, arg='a')
    r = CodeWorker().exec(step=step, context={'a': 1}, store=FileSystemStorage(tmpdir))
    assert r.returncode == 0
    assert r.stdout == ['1 written', '\n']
    assert r.stderr == []
    assert r.exception is None
    # Read the written output file.
    with open(os.path.join(tmpdir, 'out.txt'), 'r') as f:
        for line in f:
            line = line.strip()
    assert line == '1'
