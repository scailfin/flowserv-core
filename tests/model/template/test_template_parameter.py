# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for workflow template parameter declarations."""

import pytest

from flowserv.model.parameter.string import StringParameter
from flowserv.model.template.parameter import ParameterIndex

import flowserv.error as err
import flowserv.model.template.parameter as tp


def test_parameter_index_serialization():
    """Test generating parameter index from serializations."""
    p1 = StringParameter(para_id='0', name='P1', index=1)
    p2 = StringParameter(para_id='1', name='P2', index=0)
    doc = ParameterIndex.from_dict([p1.to_dict(), p2.to_dict()]).to_dict()
    parameters = ParameterIndex.from_dict(doc)
    assert len(parameters) == 2
    assert '0' in parameters
    assert '1' in parameters
    assert [p.para_id for p in parameters.sorted()] == ['1', '0']
    # Error case: Duplicate parameter.
    with pytest.raises(err.InvalidTemplateError):
        ParameterIndex.from_dict([p1.to_dict(), p1.to_dict()])
    # Error case: Unknown parameter type.
    doc = p1.to_dict()
    doc['dtype'] = 'unknown'
    with pytest.raises(err.InvalidTemplateError):
        ParameterIndex.from_dict([doc])


def test_parameter_references():
    """Test getting parameter references for a workflow specification."""
    spec = {
        'name': 'myname',
        'var': tp.VARIABLE('A'),
        'values': [
            {
                'name': 'name',
                'el': tp.VARIABLE('B'),
                'nest': {'var': tp.VARIABLE('D')}
            },
            tp.VARIABLE('C'),
            tp.VARIABLE('B'),
            3,
            'E'
        ],
        'count': 2
    }
    assert tp.get_parameter_references(spec) == set({'A', 'B', 'C', 'D'})
    # Error for nested lists.
    spec = {
        'values': [
            {
                'name': 'name',
                'el': tp.VARIABLE('B'),
                'nest': {'var': tp.VARIABLE('D')}
            },
            [tp.VARIABLE('C'), tp.VARIABLE('B')]
        ],
        'count': 2
    }
    with pytest.raises(err.InvalidTemplateError):
        tp.get_parameter_references(spec)


def test_replace_arguments():
    """Test getting parameter references for a workflow specification."""
    spec = {
        'name': 'myname',
        'var': tp.VARIABLE('A'),
        'values': [
            {
                'name': 'name',
                'el': tp.VARIABLE('B'),
                'nest': {'var': tp.VARIABLE('D')}
            },
            tp.VARIABLE('C'),
            tp.VARIABLE('B'),
            3,
            'E'
        ],
        'count': 2
    }
    parameters = ParameterIndex()
    parameters['A'] = StringParameter(para_id='A', name='P1', index=0)
    parameters['B'] = StringParameter(para_id='B', name='P2', index=1)
    parameters['C'] = StringParameter(para_id='C', name='P3', index=2)
    parameters['D'] = StringParameter(
        para_id='D',
        name='P4',
        index=3,
        default_value='default'
    )
    doc = tp.replace_args(spec, {'A': 'x', 'B': 'y', 'C': 'z'}, parameters)
    assert doc == {
        'name': 'myname',
        'var': 'x',
        'values': [
            {'name': 'name', 'el': 'y', 'nest': {'var': 'default'}},
            'z',
            'y',
            3,
            'E'
        ],
        'count': 2
    }
    # Error for missing parameter value.
    with pytest.raises(err.MissingArgumentError):
        tp.replace_args(spec, {'A': 'x', 'B': 'y'}, parameters)
    # Error for nested lists.
    with pytest.raises(err.InvalidTemplateError):
        spec = {'values': ['A', [2, 3]]}
        tp.replace_args(spec, {'A': 'x', 'B': 'y'}, parameters)


def test_variable_helpers():
    """Test helper functions for parameter references."""
    var = tp.VARIABLE('myvar')
    assert tp.is_parameter(var)
    assert tp.NAME(var) == 'myvar'
    assert not tp.is_parameter(tp.NAME(var))
