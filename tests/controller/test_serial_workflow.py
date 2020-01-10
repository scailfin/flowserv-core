# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Test functionality of the serial workflow module."""

import robcore.error as err
import robcore.controller.serial as serial
import robcore.model.template.parameter.declaration as pd
import robcore.model.template.parameter.util as putil


class TestSerialWorkflow(object):
    """Unit tests for modifying workflow templates that are supported by the
    serial workflow engine.
    """
    def test_modify_template(self):
        """Test modifying a workflow specification with additional parameters.
        """
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
        tmpl_parameters = putil.create_parameter_index([
            pd.parameter_declaration('inputfile', data_type=pd.DT_FILE),
            pd.parameter_declaration('sleeptime'),
            pd.parameter_declaration('greeting')
        ])
        # Add two parameters, one file and one scalar type
        add_parameters = putil.create_parameter_index([
            pd.parameter_declaration('gtfile', data_type=pd.DT_FILE),
            pd.parameter_declaration('waittime')
        ])
        mod_spec, mod_para = serial.modify_spec(
            workflow_spec=workflow_spec,
            tmpl_parameters=tmpl_parameters,
            add_parameters=add_parameters
        )
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
        for key in ['inputfile', 'sleeptime', 'greeting', 'gtfile', 'waittime']:
            assert key in mod_para
        # - Duplicate parameters get merged. No error is raised.
        add_parameters = putil.create_parameter_index([
            pd.parameter_declaration('greeting', data_type=pd.DT_FILE),
            pd.parameter_declaration('waittime')
        ])
        mod_spec, mod_para = serial.modify_spec(
            workflow_spec=workflow_spec,
            tmpl_parameters=tmpl_parameters,
            add_parameters=add_parameters
        )
        assert 'waittime' in mod_para
        add_parameters = putil.create_parameter_index([
            pd.parameter_declaration('message'),
            pd.parameter_declaration('waittime')
        ])
        mod_spec, mod_para = serial.modify_spec(
            workflow_spec=workflow_spec,
            tmpl_parameters=tmpl_parameters,
            add_parameters=add_parameters
        )
        assert 'message' in mod_para
        assert 'waittime' in mod_para
