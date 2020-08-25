# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for enumeration parameter declarations."""

import pytest

from flowserv.model.parameter.enum import EnumParameter, PARA_ENUM

import flowserv.error as err


def test_invalid_serialization():
    """Test errors for invalid serializations."""
    EnumParameter.from_dict({
        'id': '0000',
        'dtype': PARA_ENUM,
        'index': 0,
        'name': 'Options',
        'isRequired': True,
        'values': [{'name': 'A', 'value': 1}]
    }, validate=False)
    with pytest.raises(err.InvalidParameterError):
        EnumParameter.from_dict({
            'id': '0000',
            'dtype': PARA_ENUM,
            'index': 0,
            'name': 'Options',
            'isRequired': True
        })
    with pytest.raises(ValueError):
        EnumParameter.from_dict({
            'id': '0000',
            'dtype': 'string',
            'index': 0,
            'name': 'Name',
            'isRequired': True,
            'values': [{'name': 'A', 'value': 1}]
        })


def test_enum_parameter_from_dict():
    """Test generating an enumeration parameter declaration from a dictionary
    serialization.
    """
    para = EnumParameter.from_dict(
        EnumParameter.to_dict(
            EnumParameter.from_dict({
                'id': '0000',
                'dtype': PARA_ENUM,
                'name': 'Options',
                'index': 0,
                'description': 'List of options',
                'defaultValue': -1,
                'isRequired': False,
                'module': 'opts',
                'values': [
                    {'name': 'A', 'value': 1},
                    {'name': 'B', 'value': 2}
                ]
            })
        )
    )
    assert para.para_id == '0000'
    assert para.type_id == PARA_ENUM
    assert para.name == 'Options'
    assert para.index == 0
    assert para.description == 'List of options'
    assert para.default_value == -1
    assert not para.is_required
    assert para.module_id == 'opts'
    assert len(para.values) == 2
    assert para.values[0] == {'name': 'A', 'value': 1}
    assert para.values[1] == {'name': 'B', 'value': 2}


def test_enum_parameter_value():
    """Test getting argument value for a enumeration parameter."""
    values = [{'name': 'A', 'value': 1}, {'name': 'B', 'value': 2}]
    para = EnumParameter('0000', 'name', 0, values)
    assert para.to_argument(1) == 1
    assert para.to_argument(2) == 2
    with pytest.raises(err.InvalidArgumentError):
        para.to_argument('A')
    with pytest.raises(err.InvalidArgumentError):
        para.to_argument(None)
