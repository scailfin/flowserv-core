# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for parameter record declarations."""

import pytest

from flowserv.model.parameter.boolean import Bool
from flowserv.model.parameter.numeric import Int
from flowserv.model.parameter.record import Record, PARA_RECORD

import flowserv.error as err


def test_invalid_serialization():
    """Test errors for invalid serializations."""
    doc = {
        'name': '0000',
        'dtype': PARA_RECORD,
        'index': 0,
        'label': 'Record',
        'isRequired': True,
        'fields': [Bool('0001').to_dict()]
    }
    # Ensure that the original document is valid.
    Record.from_dict(doc, validate=True)
    # Removing a field name will not raise a InvalidParameterError if we do not
    # validate the document but it raises a KeyError when an attempt is made to
    # access the field name.
    del doc['fields'][0]['name']
    with pytest.raises(KeyError):
        Record.from_dict(doc, validate=False)
    with pytest.raises(err.InvalidParameterError):
        Record.from_dict(doc, validate=True)
    # If the whole field declaration is removed a KeyError is raised when
    # deserializing without validation and a InvalidParameterError is raised
    # when deserializing with validation.
    del doc['fields']
    with pytest.raises(KeyError):
        Record.from_dict(doc, validate=False)
    with pytest.raises(err.InvalidParameterError):
        Record.from_dict(doc, validate=True)
    # Error for field lists with duplicate names.
    field = {'name': 'f1', 'para': Bool('0001').to_dict()}
    doc['fields'] = [field, field]
    with pytest.raises(err.InvalidParameterError):
        Record.from_dict(doc, validate=False)


def test_record_parameter_from_dict():
    """Test generating an parameter record declaration from a dictionary
    serialization.
    """
    doc = {
        'name': '0000',
        'dtype': PARA_RECORD,
        'index': 1,
        'label': 'Record',
        'help': 'List of fields',
        'defaultValue': {'f1': True},
        'isRequired': True,
        'fields': [Bool('f1').to_dict(), Int('f2').to_dict()],
        'group': 'recs'
    }
    para = Record.from_dict(Record.to_dict(Record.from_dict(doc)))
    assert para.is_record()
    assert para.name == '0000'
    assert para.dtype == PARA_RECORD
    assert para.label == 'Record'
    assert para.index == 1
    assert para.help == 'List of fields'
    assert para.default == {'f1': True}
    assert para.required
    assert para.group == 'recs'
    assert len(para.fields) == 2
    assert 'f1' in para.fields
    assert para.fields['f1'].is_bool()
    assert 'f2' in para.fields
    assert para.fields['f2'].is_int()


def test_record_parameter_value():
    """Test getting argument value for a enumeration parameter."""
    para = Record('R', fields=[Bool('f1', required=True), Int('f2', default=10)])
    assert para.cast([{'name': 'f1', 'value': True}]) == {'f1': True, 'f2': 10}
    value = [{'name': 'f1', 'value': True}, {'name': 'f2', 'value': '5'}]
    assert para.cast(value) == {'f1': True, 'f2': 5}
    with pytest.raises(err.InvalidArgumentError):
        para.cast([{'name': 'f2', 'value': '5'}])
