# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the workflow template parser."""

import os

from flowserv.model.template.base import WorkflowTemplate

import flowserv.controller.serial.workflow.parser as parser
import flowserv.util as util


DIR = os.path.dirname(os.path.realpath(__file__))
BENCHMARK_DIR = os.path.join(DIR, '../../../.files/benchmark')
TEMPLATE_DIR = os.path.join(DIR, '../../../.files/template')
TEMPLATE_HELLOWORLD = os.path.join(TEMPLATE_DIR, './hello-world.yaml')
TEMPLATE_TOPTAGGER = os.path.join(BENCHMARK_DIR, './top-tagger.yaml')


def test_parse_hello_world_template():
    """Extract commands and output files from the 'Hello world' template."""
    template = WorkflowTemplate.from_dict(doc=util.read_object(TEMPLATE_HELLOWORLD))
    steps, args, output_files = parser.parse_template(template=template, arguments={'names': 'names.txt', 'sleeptime': 10})
    assert len(steps) == 1
    step = steps[0]
    assert step.image == 'python:2.7'
    assert len(step.commands) == 1
    assert step.commands[0] == 'python "${helloworld}" --inputfile "${inputfile}" --outputfile "${outputfile}" --sleeptime ${sleeptime}'  # noqa: E501
    assert output_files == ['results/greetings.txt']
    assert args == {'helloworld': 'code/helloworld.py', 'inputfile': 'names.txt', 'outputfile': 'results/greetings.txt', 'sleeptime': '10'}  # noqa: E501


def test_parse_top_tagger_template():
    """Test parsing the Top-Tagger template that contains parameter references
    as workflow steps.
    """
    template = WorkflowTemplate.from_dict(doc=util.read_object(TEMPLATE_TOPTAGGER))
    args = {'tagger': ('container', {'image': 'test', 'commands': ['python analyze']})}
    steps, _, _ = parser.parse_template(template=template, arguments=args)
    assert len(steps) == 2
    step = steps[0]
    assert step.image == 'test'
    assert len(step.commands) == 1
    assert step.commands[0] == 'python analyze'
