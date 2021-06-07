# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for workflow template parameter declarations."""

import pytest

from flowserv.model.parameter.string import String
from flowserv.model.template.parameter import ParameterIndex

import flowserv.error as err
import flowserv.model.template.parameter as tp


@pytest.fixture
def spec():
    """Workflow specification dictionary for test purposes."""
    return {
        'name': 'myname',
        'var1': tp.VARIABLE('A'),
        'var2': tp.VARIABLE('A'),
        'var3': tp.VARIABLE('E? e1'),
        'values': [
            {
                'name': 'name',
                'el1': tp.VARIABLE('B'),
                'el2': tp.VARIABLE('G?Z'),
                'el3': tp.VARIABLE('F'),
                'nest': {'var': tp.VARIABLE('D')}
            },
            tp.VARIABLE('C'),
            tp.VARIABLE('B'),
            3,
            'E'
        ],
        'count': 2
    }


@pytest.mark.parametrize(
    'value,args,result',
    [
        (tp.VARIABLE('A'), {'A': 1}, 1),
        (tp.VARIABLE('A ? xyz : abc'), {'A': True}, 'xyz'),
        (tp.VARIABLE('A ? xyz : abc'), {'A': 'true'}, 'xyz'),
        (tp.VARIABLE('A ? xyz : abc'), {'A': False}, 'abc'),
        (tp.VARIABLE('C'), {'A': 1}, 'default')
    ]
)
def test_expand_parameter_value(value, args, result):
    """Test expand parameter reference function."""
    parameters = ParameterIndex()
    parameters['A'] = String(name='A', label='P1', index=0)
    parameters['B'] = String(name='B', label='P2', index=0)
    parameters['C'] = String(
        name='C',
        label='P3',
        index=2,
        default='default'
    )
    assert tp.expand_value(value, args, parameters) == result


@pytest.mark.parametrize(
    'variable,name',
    [(' ABC ', 'ABC'), ('A', 'A'), ('name?B:C', 'name'), ('n ? A : A', 'n')]
)
def test_extract_parameter_name(variable, name):
    """Test function to extract the parameter name from a template parameter
    string.
    """
    assert tp.get_name(tp.VARIABLE(variable)) == name


@pytest.mark.parametrize(
    'value,result',
    [
        (tp.VARIABLE('A'), 'A'),
        (tp.VARIABLE('A?x:y'), 'x'),
        (tp.VARIABLE('B?x:y'), 'x'),
        (tp.VARIABLE('C?x:y'), 'y'),
        (tp.VARIABLE('A? x : y'), 'x'),
        (tp.VARIABLE('D? x : y'), 'y'),
        (tp.VARIABLE('A?x'), 'x'),
        (tp.VARIABLE('C?x'), None)
    ]
)
def test_get_parameter_value(value, result):
    """Test getting parameter reference value for conditional and unconditional
    parameter references.
    """
    args = {'A': True, 'B': 'true', 'C': 1, 'D': {'A': True}}
    assert tp.get_value(value=value, arguments=args) == result


@pytest.mark.parametrize(
    'cmd,placeholders',
    [
        ('abcdefg', set()),
        ('abc${d}ef $g\t$e', {'d', 'g', 'e'}),
        ('abc$${d}ef $g\t$g', {'g'}),
        ('${java} -jar $jarfile', {'java', 'jarfile'})
    ]
)
def test_get_placeholders(cmd, placeholders):
    assert tp.placeholders(cmd) == placeholders


@pytest.mark.parametrize(
    'value',
    [(tp.VARIABLE('B?x')), (tp.VARIABLE('B?x:y')), (tp.VARIABLE('a?x'))]
)
def test_get_parameter_value_exception(value):
    """Test exception when referencing unknown parameter in conditional
    parameter reference.
    """
    args = {'A': True}
    with pytest.raises(err.MissingArgumentError):
        assert tp.get_value(value=value, arguments=args)


def test_init_parameter_index():
    """Test initializing the parameter index from a given list of parameters."""
    assert len(ParameterIndex()) == 0
    assert len(ParameterIndex(parameters=[String('A'), String('B')])) == 2
    with pytest.raises(err.InvalidTemplateError):
        ParameterIndex(parameters=[String('A'), String('B'), String('A')])


def test_parameter_index_serialization():
    """Test generating parameter index from serializations."""
    p1 = String(name='0', label='P1', index=1)
    p2 = String(name='1', label='P2', index=0)
    doc = ParameterIndex.from_dict([p1.to_dict(), p2.to_dict()]).to_dict()
    parameters = ParameterIndex.from_dict(doc)
    assert len(parameters) == 2
    assert '0' in parameters
    assert '1' in parameters
    assert [p.name for p in parameters.sorted()] == ['1', '0']
    # Error case: Duplicate parameter.
    with pytest.raises(err.InvalidTemplateError):
        ParameterIndex.from_dict([p1.to_dict(), p1.to_dict()])
    # Error case: Unknown parameter type.
    doc = p1.to_dict()
    doc['dtype'] = 'unknown'
    with pytest.raises(err.InvalidParameterError):
        ParameterIndex.from_dict([doc])


def test_parameter_references(spec):
    """Test getting parameter references for a workflow specification."""
    params = tp.get_parameter_references(spec)
    assert params == set({'A', 'B', 'C', 'D', 'E', 'F', 'G'})
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


def test_replace_arguments(spec):
    """Test getting parameter references for a workflow specification."""
    parameters = ParameterIndex()
    parameters['A'] = String(name='A', label='P1', index=0)
    parameters['B'] = String(name='B', label='P2', index=1)
    parameters['C'] = String(name='C', label='P3', index=2)
    parameters['D'] = String(name='D', label='P4', index=3, default='default')
    parameters['E'] = String(name='E', label='P5', index=4)
    parameters['F'] = String(name='F', label='P6', index=5)
    parameters['G'] = String(name='G', label='P7', index=6)
    doc = tp.replace_args(
        spec,
        arguments={'A': 'x', 'B': 'y', 'C': 'z', 'E': True, 'F': 'b', 'G': 'c'},
        parameters=parameters
    )
    assert doc == {
        "name": "myname",
        "var1": "x",
        "var2": "x",
        "var3": "e1",
        "values": [
            {
                "name": "name",
                "el1": "y",
                "el2": None,
                "el3": "b",
                "nest": {
                    "var": "default"
                }
            },
            "z",
            "y",
            3,
            "E"
        ],
        "count": 2
    }
    # Error for missing parameter value.
    with pytest.raises(err.MissingArgumentError):
        tp.replace_args(spec, {'A': 'x', 'B': 'y'}, parameters)
    # Error for nested lists.
    with pytest.raises(err.InvalidTemplateError):
        spec = {'values': ['A', [2, 3]]}
        tp.replace_args(spec, {'A': 'x', 'B': 'y'}, parameters)
