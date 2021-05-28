# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the worker factory."""

import pytest

from flowserv.controller.worker.code import CodeWorker
from flowserv.controller.worker.docker import DockerWorker
from flowserv.controller.worker.manager import WorkerPool, Code, Docker, Subprocess
from flowserv.controller.worker.subprocess import SubprocessWorker
from flowserv.model.workflow.step import CodeStep, ContainerStep
from flowserv.volume.manager import DEFAULT_STORE

import flowserv.error as err


@pytest.mark.parametrize(
    'step,cls',
    [
        (ContainerStep(identifier='test', image='test'), SubprocessWorker),
        (ContainerStep(identifier='test', image='test'), SubprocessWorker),
        (CodeStep(identifier='test', func=lambda x: x), CodeWorker)
    ]
)
def test_get_default_worker(step, cls):
    """Test getting a default worker for a workflow step that has no manager
    explicitly assigned to it.
    """
    factory = WorkerPool(workers=[])
    assert isinstance(factory.get_default_worker(step), cls)


def test_get_worker_error():
    """Test error when accessing worker with unknown identifier."""
    step = ContainerStep(identifier='test', image='test')
    factory = WorkerPool(workers=[], managers={'test': 'test'})
    with pytest.raises(err.UnknownObjectError):
        factory.get(step)
    # Manipulate the worker type to get an error for unknown type.
    doc = Code(identifier='test')
    doc['type'] = 'unknown'
    factory = WorkerPool(workers=[doc], managers={'test': 'test'})
    with pytest.raises(ValueError):
        factory.get(step)
    # Manipulate the step type to get an error for unknown type.
    step.step_type = 'unknown'
    factory = WorkerPool(workers=[])
    with pytest.raises(ValueError):
        factory.get(step)


@pytest.mark.parametrize(
    'doc,step,cls',
    [
        (Subprocess(identifier='test'), ContainerStep(identifier='test', image='test'), SubprocessWorker),
        (Docker(identifier='test'), ContainerStep(identifier='test', image='test'), DockerWorker),
        (Code(identifier='test'), CodeStep(identifier='test', func=lambda x: x), CodeWorker)
    ]
)
def test_get_worker_instance(doc, step, cls):
    """Test creating worker instances from specification documents."""
    factory = WorkerPool(workers=[doc], managers={step.name: doc['id']})
    worker = factory.get(step)
    assert isinstance(worker, cls)
    # Run twice to account for the cached object.
    assert factory.get(step) == worker


def test_init_empty():
    """Test creating a worker factory from an empty dictionary."""
    factory = WorkerPool(workers=list())
    assert len(factory._workerspecs) == 0


def test_worker_spec_seriaization():
    """Test helper function for generating dictionary serializations for worker
    specifications.
    """
    # -- Config without additional arguments. ---------------------------------
    doc = Code(identifier='D1')
    assert doc == {'id': 'D1', 'type': 'code', 'env': [], 'vars': []}
    doc = Docker(identifier='D1')
    assert doc == {'id': 'D1', 'type': 'docker', 'env': [], 'vars': []}
    doc = Subprocess(identifier='S1')
    assert doc == {'id': 'S1', 'type': 'subprocess', 'env': [], 'vars': []}
    # -- Config with arguments ------------------------------------------------
    doc = Code(identifier='D1', volume='v1')
    assert doc == {
        'id': 'D1',
        'type': 'code',
        'env': [],
        'vars': [],
        'volume': 'v1'
    }
    vars = {'x': 1}
    env = {'TEST_ENV': 'abc'}
    doc = Docker(variables=vars, env=env, identifier='D2', volume='v1')
    assert doc == {
        'id': 'D2',
        'type': 'docker',
        'env': [{'key': 'TEST_ENV', 'value': 'abc'}],
        'vars': [{'key': 'x', 'value': 1}],
        'volume': 'v1'
    }
    doc = Subprocess(variables=vars, env=env, identifier='S2', volume='v1')
    assert doc == {
        'id': 'S2',
        'type': 'subprocess',
        'env': [{'key': 'TEST_ENV', 'value': 'abc'}],
        'vars': [{'key': 'x', 'value': 1}],
        'volume': 'v1'
    }
