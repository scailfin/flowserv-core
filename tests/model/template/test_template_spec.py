# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for functionality of the template base module."""

import pytest

from flowserv.model.parameter.string import StringParameter, PARA_STRING
from flowserv.model.template.base import WorkflowTemplate
from flowserv.model.template.parameter import ParameterIndex

import flowserv.error as err
import flowserv.model.template.parameter as tp


def test_template_serialization():
    """Test creating template instances from serializations."""
    # Minimal template specification.
    doc = {'workflow': dict()}
    doc = WorkflowTemplate.from_dict(doc, '/dev/null').to_dict()
    template = WorkflowTemplate.from_dict(doc, '/dev/null')
    assert template.workflow_spec == dict()
    assert template.parameters == ParameterIndex()
    assert template.sourcedir == '/dev/null'
    # Maximal template specification.
    doc = {
        'workflow': {'inputs': [tp.VARIABLE('A'), 'B', 'C']},
        'parameters': [
            StringParameter(para_id='A', name='P1', index=0).to_dict()
        ],
        'modules': [
            {'id': '0', 'name': 'G1', 'index': 0},
            {'id': '1', 'name': 'G2', 'index': 1}
        ],
        'postproc': {'workflow': dict(), 'inputs': {'files': ['D', 'E']}},
        'results': {
            'file': 'results/analytics.json',
            'schema': [{'id': '0', 'name': 'col0', 'type': PARA_STRING}]
        }
    }
    doc = WorkflowTemplate.from_dict(doc, '/dev/null').to_dict()
    template = WorkflowTemplate.from_dict(doc, '/dev/null')
    assert template.workflow_spec == {'inputs': [tp.VARIABLE('A'), 'B', 'C']}
    assert len(template.parameters) == 1
    assert len(template.modules) == 2
    assert template.postproc_spec['workflow'] == dict()
    # No error for invalid document only if validate is not set to False.
    para = StringParameter(para_id='0', name='P1', index=0).to_dict()
    para['addOn'] = 1
    doc = {
        'workflow': {'inputs': ['A', 'B', 'C']},
        'parameters': [para],
        'modules': [
            {'id': '0', 'name': 'G1', 'index': 0, 'sortDesc': True},
            {'id': '1', 'name': 'G2', 'index': 1}
        ],
        'postproc': {'inputs': {'files': ['D', 'E']}}
    }
    WorkflowTemplate.from_dict(doc, '/dev/null', validate=False)
    with pytest.raises(err.InvalidParameterError):
        WorkflowTemplate.from_dict(doc, '/dev/null')
    # Error for missing workflow specification.
    with pytest.raises(err.InvalidTemplateError):
        WorkflowTemplate.from_dict(dict(), '/dev/null')
    # Error for unknown parameter.
    with pytest.raises(err.UnknownParameterError):
        doc = {
            'workflow': {'inputs': [tp.VARIABLE('0'), 'B', 'C']},
            'parameters': [
                StringParameter(para_id='A', name='P1', index=0).to_dict()
            ]
        }
        WorkflowTemplate.from_dict(doc, '/dev/null')


def test_validate_arguments():
    """Test validating a given set of arguments against the parameters in a
    workflow template.
    """
    parameters = ParameterIndex.from_dict([
        StringParameter(
            para_id='A',
            name='P1',
            index=0,
            is_required=True
        ).to_dict(),
        StringParameter(
            para_id='B',
            name='P2',
            index=1,
            default_value='X'
        ).to_dict()
    ])
    template = WorkflowTemplate(
        workflow_spec=dict(),
        sourcedir='/dev/null',
        parameters=parameters
    )
    template.validate_arguments({'A': 1, 'B': 0})
    template.validate_arguments({'A': 1})
    with pytest.raises(err.MissingArgumentError):
        template.validate_arguments({'B': 1})
