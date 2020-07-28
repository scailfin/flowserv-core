# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for string parameter declarations."""

import pytest

from flowserv.model.parameter.string import StringParameter, PARA_STRING

import flowserv.error as err


def test_invalid_serialization():
    """Test errors for invalid serializations."""
    with pytest.raises(err.InvalidParameterError):
        StringParameter.from_dict({
            'id': '0000',
            'type': PARA_STRING,
        })
    StringParameter.from_dict({
        'id': '0000',
        'type': 'unknown',
        'index': 0,
        'name': 'Name',
        'isRequired': True
    }, validate=False)
    with pytest.raises(ValueError):
        StringParameter.from_dict({
            'id': '0000',
            'type': 'unknown',
            'index': 0,
            'name': 'Name',
            'isRequired': True
        })
    with pytest.raises(ValueError):
        StringParameter.from_dict({
            'id': None,
            'type': PARA_STRING,
            'name': 'Firstname',
            'index': 1,
            'description': 'Your first name',
            'defaultValue': 'Alice',
            'isRequired': True,
            'module': 'person'
        })


def test_string_parameter_from_dict():
    """Test generating a string parameter declaration from a dictionary
    serialization.
    """
    para = StringParameter.from_dict(
        StringParameter.to_dict(
            StringParameter.from_dict({
                'id': '0000',
                'type': PARA_STRING,
                'name': 'Firstname',
                'index': 1,
                'description': 'Your first name',
                'defaultValue': 'Alice',
                'isRequired': True,
                'module': 'person'
            })
        )
    )
    assert para.para_id == '0000'
    assert para.type_id == PARA_STRING
    assert para.name == 'Firstname'
    assert para.index == 1
    assert para.description == 'Your first name'
    assert para.default_value == 'Alice'
    assert para.is_required
    assert para.module_id == 'person'


def test_string_parameter_value():
    """Test getting argument value for a string parameter."""
    para = StringParameter('0000', 'name', 0)
    assert para.to_argument(2) == '2'
    assert para.to_argument('ABC') == 'ABC'
    assert para.to_argument(None) == 'None'
    para = StringParameter('0000', 'name', 0, is_required=True)
    assert para.to_argument(2) == '2'
    assert para.to_argument('ABC') == 'ABC'
    with pytest.raises(err.InvalidArgumentError):
        para.to_argument(None)
