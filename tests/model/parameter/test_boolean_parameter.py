# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for Boolean parameter declarations."""

import pytest

from flowserv.model.parameter.boolean import BoolParameter, PARA_BOOL

import flowserv.error as err


def test_invalid_serialization():
    """Test errors for invalid serializations."""
    with pytest.raises(err.InvalidParameterError):
        BoolParameter.from_dict({
            'id': '0000',
            'type': PARA_BOOL,
        })
    BoolParameter.from_dict({
        'id': '0000',
        'type': 'string',
        'index': 0,
        'name': 'Name',
        'isRequired': True
    }, validate=False)
    with pytest.raises(ValueError):
        BoolParameter.from_dict({
            'id': '0000',
            'type': 'unknown',
            'index': 0,
            'name': 'Name',
            'isRequired': True
        })


def test_boolean_parameter_from_dict():
    """Test generating a Boolean parameter declaration from a dictionary
    serialization.
    """
    para = BoolParameter.from_dict(
        BoolParameter.to_dict(
            BoolParameter.from_dict({
                'id': '0000',
                'type': PARA_BOOL,
                'name': 'Agree',
                'index': 10,
                'description': 'Do you agree?',
                'defaultValue': False,
                'isRequired': True,
                'module': 'contract'
            })
        )
    )
    assert para.para_id == '0000'
    assert para.type_id == PARA_BOOL
    assert para.name == 'Agree'
    assert para.index == 10
    assert para.description == 'Do you agree?'
    assert not para.default_value
    assert para.is_required
    assert para.module_id == 'contract'


def test_boolean_parameter_value():
    """Test getting argument value for a boolean parameter."""
    para = BoolParameter('0000', 'name', 0)
    # Values that convert to True.
    assert para.to_argument(True)
    assert para.to_argument(1)
    assert para.to_argument('1')
    assert para.to_argument('T')
    assert para.to_argument('true')
    # Values that convert to False
    assert not para.to_argument(False)
    assert not para.to_argument(0)
    assert not para.to_argument('0')
    assert not para.to_argument('')
    assert not para.to_argument('f')
    assert not para.to_argument('FALSE')
    assert not para.to_argument(None)
    # Values that raise errors
    with pytest.raises(err.InvalidArgumentError):
        para.to_argument('yes')
    with pytest.raises(err.InvalidArgumentError):
        para.to_argument(10)
    with pytest.raises(err.InvalidArgumentError):
        para.to_argument('   ')
