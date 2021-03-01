# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the worker factory."""

from jsonschema.exceptions import ValidationError

import os
import pytest

from flowserv.controller.worker.docker import DockerWorker
from flowserv.controller.worker.factory import WorkerFactory, Docker, Subprocess
from flowserv.controller.worker.subprocess import SubprocessWorker

import flowserv.util as util


# Config files.
DIR = os.path.dirname(os.path.realpath(__file__))
CONFIG_DIR = os.path.join(DIR, '../../.files/controller')
JSON_FILE = os.path.join(CONFIG_DIR, 'worker.json')
YAML_FILE = os.path.join(CONFIG_DIR, 'worker.yaml')


def callme():
    return 'called'


def test_config_helper():
    """Test helper function for generating worker configurations."""
    # -- Config without additional arguments. ---------------------------------
    doc = Docker()
    assert doc == {'worker': 'docker'}
    doc = Subprocess()
    assert doc == {'worker': 'subprocess'}
    # -- Config with arguments ------------------------------------------------
    vars = {'x': 1}
    env = {'TEST_ENV': 'abc'}
    doc = Docker(variables=vars, env=env)
    assert doc == {'worker': 'docker', 'arguments': {'variables': vars, 'env': env}}
    doc = Subprocess(variables=vars, env=env)
    assert doc == {'worker': 'subprocess', 'arguments': {'variables': vars, 'env': env}}


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
                'worker': {'className': 'SubprocessWorker', 'moduleName': 'flowserv.controller.worker.subprocess'}
            }},
            SubprocessWorker
        ),
        (
            {'test': {'worker': {'className': 'DockerWorker', 'moduleName': 'flowserv.controller.worker.docker'}}},
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


def test_load_config_from_file():
    """Test loading worker factory configuration from a file."""
    # Passing the file content directly to the object constructor should work
    # the same as using the static load method.
    worker = WorkerFactory(util.read_object(JSON_FILE)).get('test')
    assert worker.variables['a'] == 0


def test_load_config_from_json_file():
    """Test loading worker factory configuration from a Json file."""
    worker = WorkerFactory.load_json(JSON_FILE).get('test')
    assert worker.variables['a'] == 0
    # Passing the file content directly to the object constructor should yield
    # the same result.
    worker = WorkerFactory(util.read_object(JSON_FILE)).get('test')
    assert worker.variables['a'] == 0


def test_load_config_from_yaml_file():
    """Test loading worker factory configuration from a Yaml file."""
    worker = WorkerFactory.load_yaml(YAML_FILE).get('test')
    assert worker.variables['a'] == 0
