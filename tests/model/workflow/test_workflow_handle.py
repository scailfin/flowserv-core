# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for workflow descriptors and handles."""


from flowserv.model.parameter.base import ParameterGroup
from flowserv.model.template.base import WorkflowTemplate
from flowserv.model.template.schema import ResultSchema
from flowserv.model.workflow.base import WorkflowDescriptor, WorkflowHandle
from flowserv.tests.parameter import StringParameter


def test_workflow_descriptor():
    """Unit tests for initializing workflow handles."""
    # Minimal workfow descriptor constructor
    wf = WorkflowDescriptor(identifier='ABC')
    assert wf.identifier == 'ABC'
    assert wf.name == 'ABC'
    assert wf.description is None
    assert not wf.has_description()
    assert wf.get_description() == ''
    assert not wf.has_instructions()
    assert wf.instructions is None
    assert wf.get_instructions() == ''
    # Maximal workfow descriptor constructor
    wf = WorkflowDescriptor(
        identifier='ABC',
        name='XYZ',
        description='Some description',
        instructions='Some instructions'
    )
    assert wf.identifier == 'ABC'
    assert wf.name == 'XYZ'
    assert wf.description is not None
    assert wf.has_description()
    assert wf.get_description() == 'Some description'
    assert wf.has_instructions()
    assert wf.instructions is not None
    assert wf.get_instructions() == 'Some instructions'


def test_workflow_handle():
    """Unit tests for workflow handles."""
    # Minimal workflow handle constructor
    wf = WorkflowHandle(
        identifier='ABC',
        template=WorkflowTemplate(
            workflow_spec={'a': 1},
            sourcedir='/dev/null'
        ),
        con=None
    )
    assert wf.identifier == 'ABC'
    assert wf.name == 'ABC'
    assert wf.description is None
    assert not wf.has_description()
    assert wf.get_description() == ''
    assert not wf.has_instructions()
    assert wf.instructions is None
    assert wf.get_instructions() == ''
    assert len(wf.resources) == 0
    # Get modified workflow template
    wf = WorkflowHandle(
        identifier='ABC',
        name='XYZ',
        description='Some description',
        instructions='Some instructions',
        template=WorkflowTemplate(
            workflow_spec={'a': 1},
            postproc_spec={'env': 'bash', 'commands': ['echo $HELLO_WORLD']},
            parameters=[
                StringParameter('para1'),
                StringParameter('para2')
            ],
            modules=[
                ParameterGroup(identifier='m1', name='M1', index=0)
            ],
            result_schema=ResultSchema(result_file='results/analytics.json'),
            sourcedir='/dev/null'
        ),
        con=None
    )
    assert wf.identifier == 'ABC'
    assert wf.name == 'XYZ'
    assert wf.get_description() == 'Some description'
    assert wf.get_instructions() == 'Some instructions'
    # Get unmodified template
    template = wf.get_template()
    assert template.workflow_spec == {'a': 1}
    assert template.postproc_spec == {'env': 'bash', 'commands': ['echo $HELLO_WORLD']}
    assert len(template.parameters) == 2
    assert len(template.modules) == 1
    assert template.modules[0].name == 'M1'
    assert template.get_schema().result_file == 'results/analytics.json'
    assert template.sourcedir == '/dev/null'
    # Get template with modified workflow specification
    template = wf.get_template(workflow_spec={'b': 2})
    assert template.workflow_spec == {'b': 2}
    assert template.postproc_spec == {'env': 'bash', 'commands': ['echo $HELLO_WORLD']}
    assert len(template.parameters) == 2
    assert len(template.modules) == 1
    assert template.modules[0].name == 'M1'
    assert template.get_schema().result_file == 'results/analytics.json'
    assert template.sourcedir == '/dev/null'
    # Get template with modified parameter list
    template = wf.get_template(parameters=[StringParameter('para1')])
    assert template.workflow_spec == {'a': 1}
    assert template.postproc_spec == {'env': 'bash', 'commands': ['echo $HELLO_WORLD']}
    assert len(template.parameters) == 1
    assert len(template.modules) == 1
    assert template.modules[0].name == 'M1'
    assert template.get_schema().result_file == 'results/analytics.json'
    assert template.sourcedir == '/dev/null'
    # Get template with modified workflow specification and parameter list
    template = wf.get_template(
        workflow_spec={'c': 3},
        parameters=[
            StringParameter('para1'),
            StringParameter('para2'),
            StringParameter('para3')
        ]
    )
    assert template.workflow_spec == {'c': 3}
    assert template.postproc_spec == {'env': 'bash', 'commands': ['echo $HELLO_WORLD']}
    assert len(template.parameters) == 3
    assert len(template.modules) == 1
    assert template.modules[0].name == 'M1'
    assert template.get_schema().result_file == 'results/analytics.json'
    assert template.sourcedir == '/dev/null'
