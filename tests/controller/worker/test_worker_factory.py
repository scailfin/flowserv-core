# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the worker factory."""

from jsonschema.exceptions import ValidationError

import pytest

from flowserv.controller.serial.worker.docker import DockerWorker
from flowserv.controller.serial.worker.factory import WorkerFactory
from flowserv.controller.serial.worker.subprocess import SubprocessWorker


def callme():
    return 'called'


def test_eval_arg_func():
    """Test creating a worker factory from a dictionary that contains
    argument specifications that are callables.
    """
    doc = {
        'test': {
            'image': 'test',
            'worker': 'subprocess',
            'args': {
                'a': callme,
                'b': 1
            }
        },
        'dummy': {'image': 'dummy', 'worker': {'className': 'a', 'moduleName': 'b'}}
    }
    factory = WorkerFactory(config=doc, validate=True)
    assert factory.config['test']['args'] == {'a': 'called', 'b': 1}
    assert factory.config['dummy'].get('args') is None


@pytest.mark.parametrize(
    'doc,cls',
    [
        ({'test': {'worker': 'subprocess', 'args': {'variables': {'python': 'py'}}}}, SubprocessWorker),
        ({'test': {'worker': 'docker'}}, DockerWorker),
        ({'default': {'worker': 'docker'}}, SubprocessWorker),
        (
            {'test': {
                'worker': {'className': 'SubprocessWorker', 'moduleName': 'flowserv.controller.serial.worker.subprocess'}
            }},
            SubprocessWorker
        ),
        (
            {'test': {'worker': {'className': 'DockerWorker', 'moduleName': 'flowserv.controller.serial.worker.docker'}}},
            DockerWorker
        )
    ]
)
def test_get_worker_instance(doc, cls):
    """Test creating worker instances from specification documents."""
    factory = WorkerFactory(config=doc)
    worker = factory.get('test')
    assert isinstance(worker, cls)
    # Run twice to account for the cached object.
    assert factory.get('test') == worker


@pytest.mark.parametrize('validate', [True, False])
def test_init_empty(validate):
    """Test creating a worker factory from an empty dictionary."""
    factory = WorkerFactory(config=dict(), validate=validate)
    assert len(factory.config) == 0


@pytest.mark.parametrize(
    'config',
    [
        {'test': {}},
        {'test': {'image': 'test'}},
        {'test': {'image': 'test', 'worker': {'className': 'a'}}},
        {'test': {'image': 'test', 'worker': 'dummy'}},
    ]
)
def test_invalid_config(config):
    """Test errors when creating factory from invalid worker specifications."""
    # No error when validate is False
    WorkerFactory(config=config)
    with pytest.raises(ValidationError):
        WorkerFactory(config=config, validate=True)
