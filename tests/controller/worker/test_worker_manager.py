# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the worker factory."""

import pytest

from flowserv.controller.worker.docker import DockerWorker
from flowserv.controller.worker.manager import WorkerPool, Docker, Subprocess
from flowserv.controller.worker.subprocess import SubprocessWorker

import flowserv.error as err


def test_worker_spec_seriaization():
    """Test helper function for generating dictionary serializations for worker
    specifications.
    """
    # -- Config without additional arguments. ---------------------------------
    doc = Docker(identifier='D1')
    assert doc == {'id': 'D1', 'type': 'docker', 'env': [], 'vars': []}
    doc = Subprocess(identifier='S1')
    assert doc == {'id': 'S1', 'type': 'subprocess', 'env': [], 'vars': []}
    # -- Config with arguments ------------------------------------------------
    vars = {'x': 1}
    env = {'TEST_ENV': 'abc'}
    doc = Docker(variables=vars, env=env, identifier='D2')
    assert doc == {
        'id': 'D2',
        'type': 'docker',
        'env': [{'key': 'TEST_ENV', 'value': 'abc'}],
        'vars': [{'key': 'x', 'value': 1}]
    }
    doc = Subprocess(variables=vars, env=env, identifier='S2')
    assert doc == {
        'id': 'S2',
        'type': 'subprocess',
        'env': [{'key': 'TEST_ENV', 'value': 'abc'}],
        'vars': [{'key': 'x', 'value': 1}]
    }


def test_get_worker_error():
    """Test error when accessing worker with unknown identifier."""
    factory = WorkerPool(workers=[])
    with pytest.raises(err.UnknownObjectError):
        factory.get('test')
    factory = WorkerPool(workers=[Subprocess(identifier='s1')])
    with pytest.raises(err.UnknownObjectError):
        factory.get('test')
    factory = WorkerPool(workers=[{'id': 'test', 'type': 'unknown'}])
    with pytest.raises(ValueError):
        factory.get('test')


@pytest.mark.parametrize(
    'doc,cls',
    [
        (Subprocess(identifier='test'), SubprocessWorker),
        (Docker(identifier='test'), DockerWorker),
    ]
)
def test_get_worker_instance(doc, cls):
    """Test creating worker instances from specification documents."""
    factory = WorkerPool(workers=[doc])
    worker = factory.get('test')
    assert isinstance(worker, cls)
    # Run twice to account for the cached object.
    assert factory.get('test') == worker


def test_init_empty():
    """Test creating a worker factory from an empty dictionary."""
    factory = WorkerPool(workers=list())
    assert len(factory._workerspecs) == 0
