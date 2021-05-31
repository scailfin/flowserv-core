# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the workflow template parser."""

import os
import pytest

from flowserv.model.parameter.actor import ActorValue
from flowserv.model.template.base import WorkflowTemplate
from flowserv.model.template.parameter import ParameterIndex

import flowserv.controller.serial.workflow.parser as parser
import flowserv.util as util


DIR = os.path.dirname(os.path.realpath(__file__))
BENCHMARK_DIR = os.path.join(DIR, '../../../.files/benchmark')
TEMPLATE_HELLOWORLD = os.path.join(BENCHMARK_DIR, 'helloworld', 'benchmark.yaml')
TEMPLATE_NOTEBOOK = os.path.join(BENCHMARK_DIR, 'helloworld', './benchmark-with-notebook.yaml')
TEMPLATE_TOPTAGGER = os.path.join(BENCHMARK_DIR, './top-tagger.yaml')


def test_parse_code_step():
    """Test parsing specification for a workflow with a code step."""
    doc = {'steps': [{
        'name': 'code_step',
        'action': {
            'func': 'flowserv.tests.worker.a_plus_b',
            'arg': 'z',
            'variables': [{'arg': 'a', 'var': 'val1'}, {'arg': 'b', 'var': 'val2'}]
        }
    }]}
    template = WorkflowTemplate(workflow_spec=doc, parameters=ParameterIndex())
    steps, _, _ = parser.parse_template(template=template, arguments=dict())
    assert len(steps) == 1
    step = steps[0]
    assert step.func(2, 3) == 5
    assert step.arg == 'z'
    assert step.varnames == {'a': 'val1', 'b': 'val2'}


def test_parse_hello_world_template():
    """Extract commands and output files from the 'Hello world' template."""
    template = WorkflowTemplate.from_dict(doc=util.read_object(TEMPLATE_HELLOWORLD))
    steps, args, output_files = parser.parse_template(template=template, arguments={'names': 'names.txt', 'sleeptime': 10})
    assert len(steps) == 1
    step = steps[0]
    assert step.image == 'python:3.7'
    assert len(step.commands) == 2
    assert output_files == ['results/greetings.txt', 'results/analytics.json']
    assert args == {'inputfile': 'names.txt', 'outputfile': 'results/greetings.txt', 'sleeptime': '10', 'greeting': 'Hello'}


def test_parse_hello_world_notebook_template():
    """Extract commands and output files from the 'Hello world' template
    that included a notebook step.
    """
    template = WorkflowTemplate.from_dict(doc=util.read_object(TEMPLATE_NOTEBOOK))
    steps, args, output_files = parser.parse_template(template=template, arguments={'greeting': 'Hey'})
    assert len(steps) == 2
    step = steps[0]
    assert step.notebook == 'notebooks/HelloWorld.ipynb'
    assert step.inputs == ['data/names.txt', 'notebooks/HelloWorld.ipynb']
    assert step.outputs == ['results/greetings.txt']
    assert output_files == ['results/greetings.txt', 'results/analytics.json']
    assert args == {'inputfile': 'data/names.txt', 'outputfile': 'results/greetings.txt', 'greeting': 'Hey'}


def test_parse_top_tagger_template():
    """Test parsing the Top-Tagger template that contains parameter references
    as workflow steps.
    """
    template = WorkflowTemplate.from_dict(doc=util.read_object(TEMPLATE_TOPTAGGER))
    doc = {'environment': 'test', 'commands': ['python analyze']}
    args = {'tagger': ActorValue(spec=doc)}
    steps, _, _ = parser.parse_template(template=template, arguments=args)
    assert len(steps) == 2
    step = steps[0]
    assert step.image == 'test'
    assert step.commands == ['python analyze']


def test_parse_workflow_spec_error():
    """Test error for unknown workflow step when parsing a serial workflow
    specification.
    """
    doc = {'steps': [{'name': 'S1', 'action': {'type': 'undefined'}}]}
    template = WorkflowTemplate(workflow_spec=doc, parameters=ParameterIndex())
    with pytest.raises(ValueError):
        parser.parse_template(template=template, arguments=dict())
