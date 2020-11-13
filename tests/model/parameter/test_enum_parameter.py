# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for enumeration parameter declarations."""

import pytest

from flowserv.model.parameter.enum import Select, Option, PARA_SELECT

import flowserv.error as err


def test_invalid_serialization():
    """Test errors for invalid serializations."""
    Select.from_dict({
        'name': '0000',
        'dtype': PARA_SELECT,
        'index': 0,
        'label': 'Options',
        'isRequired': True,
        'values': [Option('A', 1)]
    }, validate=False)
    with pytest.raises(err.InvalidParameterError):
        Select.from_dict({
            'name': '0000',
            'dtype': PARA_SELECT,
            'index': 0,
            'label': 'Options',
            'isRequired': True
        })
    with pytest.raises(ValueError):
        Select.from_dict({
            'name': '0000',
            'dtype': 'string',
            'index': 0,
            'label': 'Name',
            'isRequired': True,
            'values': [Option('A', 1)]
        })


def test_enum_parameter_from_dict():
    """Test generating an enumeration parameter declaration from a dictionary
    serialization.
    """
    para = Select.from_dict(
        Select.to_dict(
            Select.from_dict({
                'name': '0000',
                'dtype': PARA_SELECT,
                'label': 'Options',
                'index': 0,
                'help': 'List of options',
                'defaultValue': -1,
                'isRequired': False,
                'group': 'opts',
                'values': [Option('A', 1), Option('B', 2, default=True)]
            })
        )
    )
    assert para.is_select()
    assert para.name == '0000'
    assert para.dtype == PARA_SELECT
    assert para.label == 'Options'
    assert para.index == 0
    assert para.help == 'List of options'
    assert para.default == -1
    assert not para.required
    assert para.group == 'opts'
    assert len(para.values) == 2
    assert para.values[0] == Option('A', 1)
    assert para.values[1] == Option('B', 2, default=True)


def test_enum_parameter_value():
    """Test getting argument value for a enumeration parameter."""
    values = [Option('A', 1), Option('B', 2)]
    para = Select('0000', values, 0)
    assert para.to_argument(1) == 1
    assert para.to_argument(2) == 2
    with pytest.raises(err.InvalidArgumentError):
        para.to_argument('A')
    with pytest.raises(err.InvalidArgumentError):
        para.to_argument(None)
