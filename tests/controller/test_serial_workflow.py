# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for template modification functionality in the serial workflow
module."""

from flowserv.controller.serial.engine import SerialWorkflowEngine
from flowserv.model.template.base import WorkflowTemplate

import flowserv.model.parameter.base as pb
import flowserv.model.parameter.declaration as pd


def test_modify_template():
    """Test modifying a workflow specification with additional parameters."""
    # Create a workflow specification and three initial parameters
    spec = {
        'type': 'serial',
        'environment': 'python:3.7'
    }
    workflow_spec = {
        'inputs': {
            'files': ['analyze.py', 'helloworld.py', '$[[names]]'],
            'parameters': {
                'inputfile': '$[[names]]',
                'sleeptime': '$[[sleeptime]]',
                'message': '$[[greeting]]'
            }
        },
        'workflow': spec
    }
    tmpl_parameters = pb.create_parameter_index([
        pd.parameter_declaration('inputfile', data_type=pd.DT_FILE),
        pd.parameter_declaration('sleeptime'),
        pd.parameter_declaration('greeting')
    ])
    template = WorkflowTemplate(
        workflow_spec=workflow_spec,
        parameters=tmpl_parameters,
        sourcedir='/dev/null'
    )
    # Create serial workflow engine
    engine = SerialWorkflowEngine()
    # Add two parameters, one file and one scalar type
    add_parameters = pb.create_parameter_index([
        pd.parameter_declaration('gtfile', data_type=pd.DT_FILE),
        pd.parameter_declaration('waittime')
    ])
    mod_template = engine.modify_template(
        template=template,
        parameters=add_parameters
    )
    mod_spec = mod_template.workflow_spec
    mod_para = mod_template.parameters
    # Ensure that the original spec has not changed
    assert len(workflow_spec['inputs']['files']) == 3
    assert len(workflow_spec['inputs']['parameters']) == 3
    assert mod_spec['workflow'] == spec
    assert len(mod_spec['inputs']['files']) == 4
    assert len(mod_spec['inputs']['parameters']) == 4
    for f in ['analyze.py', 'helloworld.py', '$[[names]]', '$[[gtfile]]']:
        assert f in mod_spec['inputs']['files']
    for p in ['inputfile', 'sleeptime', 'message', 'waittime']:
        assert p in mod_spec['inputs']['parameters']
    assert len(mod_para) == 5
    keys = ['inputfile', 'sleeptime', 'greeting', 'gtfile', 'waittime']
    for key in keys:
        assert key in mod_para
    # - Duplicate parameters get merged. No error is raised.
    add_parameters = pb.create_parameter_index([
        pd.parameter_declaration('greeting', data_type=pd.DT_FILE),
        pd.parameter_declaration('waittime')
    ])
    mod_template = engine.modify_template(
        template=template,
        parameters=add_parameters
    )
    mod_spec = mod_template.workflow_spec
    mod_para = mod_template.parameters
    assert 'waittime' in mod_para
    add_parameters = pb.create_parameter_index([
        pd.parameter_declaration('message'),
        pd.parameter_declaration('waittime')
    ])
    mod_template = engine.modify_template(
        template=template,
        parameters=add_parameters
    )
    mod_spec = mod_template.workflow_spec
    mod_para = mod_template.parameters
    assert 'message' in mod_para
    assert 'waittime' in mod_para
