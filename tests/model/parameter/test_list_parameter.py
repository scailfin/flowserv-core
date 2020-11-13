# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for list parameter declarations."""

import pytest

from flowserv.model.parameter.boolean import Bool
from flowserv.model.parameter.list import Array, PARA_LIST
from flowserv.model.parameter.numeric import Int
from flowserv.model.parameter.record import Record

import flowserv.error as err


def test_invalid_serialization():
    """Test errors for invalid serializations."""
    doc = {
        'name': '0000',
        'dtype': PARA_LIST,
        'index': 0,
        'label': 'Record',
        'isRequired': True,
        'para': Bool('0001').to_dict()
    }
    # Ensure that the original document is valid.
    Array.from_dict(doc, validate=True)
    # Removing a 'para' name will not raise a InvalidParameterError if we do not
    # validate the document but it raises a KeyError when an attempt is made to
    # access the 'para' name.
    del doc['para']
    with pytest.raises(KeyError):
        Array.from_dict(doc, validate=False)
    with pytest.raises(err.InvalidParameterError):
        Array.from_dict(doc, validate=True)


def test_list_parameter_from_dict():
    """Test generating a list parameter declaration from a dictionary
    serialization.
    """
    doc = {
        'name': '0000',
        'dtype': PARA_LIST,
        'index': 1,
        'label': 'List',
        'help': 'List of values',
        'defaultValue': [1, 2, 3],
        'isRequired': True,
        'para': Bool('f1').to_dict(),
        'group': 'arrays'
    }
    para = Array.from_dict(Array.to_dict(Array.from_dict(doc)))
    assert para.is_list()
    assert para.name == '0000'
    assert para.dtype == PARA_LIST
    assert para.label == 'List'
    assert para.index == 1
    assert para.help == 'List of values'
    assert para.default == [1, 2, 3]
    assert para.required
    assert para.group == 'arrays'
    assert para.para.name == 'f1'
    assert para.para.is_bool()


def test_list_parameter_value():
    """Test getting argument value for a list parameter."""
    para = Array('A', para=Record('R', fields=[Bool('f1'), Int('f2', default=10)]))
    value = [
        [{'name': 'f1', 'value': True}, {'name': 'f2', 'value': '5'}],
        [{'name': 'f1', 'value': False}, {'name': 'f2', 'value': '10'}]
    ]
    assert para.cast(value) == [{'f1': True, 'f2': 5}, {'f1': False, 'f2': 10}]
