# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for Boolean parameter declarations."""

import pytest

from flowserv.model.parameter.boolean import Bool, PARA_BOOL

import flowserv.error as err


def test_invalid_serialization():
    """Test errors for invalid serializations."""
    with pytest.raises(err.InvalidParameterError):
        Bool.from_dict({
            'name': '0000',
            'dtype': PARA_BOOL,
        })
    Bool.from_dict({
        'name': '0000',
        'dtype': 'string',
        'index': 0,
        'label': 'Name',
        'isRequired': True
    }, validate=False)
    with pytest.raises(ValueError):
        Bool.from_dict({
            'name': '0000',
            'dtype': 'unknown',
            'index': 0,
            'label': 'Name',
            'isRequired': True
        })


def test_boolean_parameter_from_dict():
    """Test generating a Boolean parameter declaration from a dictionary
    serialization.
    """
    para = Bool.from_dict(
        Bool.to_dict(
            Bool.from_dict({
                'name': '0000',
                'dtype': PARA_BOOL,
                'label': 'Agree',
                'index': 10,
                'help': 'Do you agree?',
                'defaultValue': False,
                'isRequired': True,
                'group': 'contract'
            })
        )
    )
    assert para.name == '0000'
    assert para.dtype == PARA_BOOL
    assert para.label == 'Agree'
    assert para.index == 10
    assert para.help == 'Do you agree?'
    assert not para.default
    assert para.required
    assert para.group == 'contract'


def test_boolean_parameter_value():
    """Test getting argument value for a boolean parameter."""
    para = Bool('0000', 0, 'name')
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
