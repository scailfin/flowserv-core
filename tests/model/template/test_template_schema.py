# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for workflow result schema objects."""

import pytest

from flowserv.model.parameter.numeric import PARA_FLOAT, PARA_INT
from flowserv.model.parameter.string import PARA_STRING
from flowserv.model.template.schema import (
    ResultColumn, ResultSchema, SortColumn
)

import flowserv.error as err


def test_result_column_path():
    """Test result column serialiations with optional query path."""
    doc = {'name': '0', 'label': 'col0', 'dtype': PARA_STRING, 'path': 'x/y/z'}
    col = ResultColumn.from_dict(ResultColumn.from_dict(doc).to_dict())
    assert col.jpath() == ['x', 'y', 'z']
    doc = {'name': '0', 'label': 'col0', 'dtype': PARA_STRING}
    col = ResultColumn.from_dict(ResultColumn.from_dict(doc).to_dict())
    assert col.jpath() == ['0']


def test_result_column_type():
    """Test data type and cast function for schema columns."""
    col = ResultColumn(column_id='0', name='0', dtype=PARA_INT)
    assert col.cast('3') == 3
    col = ResultColumn(column_id='0', name='0', dtype=PARA_FLOAT)
    assert col.cast('3.5') == 3.5
    with pytest.raises(ValueError):
        col.cast('XYZ')
    # Error for invalid column type.
    with pytest.raises(err.InvalidTemplateError):
        ResultColumn(column_id='0', name='0', dtype='file')
    col = ResultColumn(column_id='0', name='0', dtype=PARA_STRING)
    assert col.cast('3.5') == '3.5'
    assert col.cast('XYZ') == 'XYZ'


def test_schema_serialization():
    """Test creating a result schema from a valid serialization."""
    # Schema without 'Order By' clause.
    schema = ResultSchema.from_dict(ResultSchema.from_dict({
        'file': 'results/analytics.json',
        'schema': [
            {'name': '0', 'label': 'col0', 'dtype': PARA_STRING},
            {'name': '1', 'label': 'col1', 'dtype': PARA_STRING}
        ]
    }).to_dict())
    assert schema.result_file == 'results/analytics.json'
    assert len(schema.columns) == 2
    sort_col = schema.get_default_order()[0]
    assert sort_col.to_dict() == {'name': '0', 'sortDesc': True}
    # Schema with 'Order By' clause.
    schema = ResultSchema.from_dict(ResultSchema.from_dict({
        'file': 'results/analytics.json',
        'schema': [
            {'name': '0', 'label': 'col0', 'dtype': PARA_STRING},
            {'name': '1', 'label': 'col1', 'dtype': PARA_STRING}
        ],
        'orderBy': [{'name': '1', 'sortDesc': False}]
    }).to_dict())
    assert schema.result_file == 'results/analytics.json'
    assert len(schema.columns) == 2
    sort_col = schema.get_default_order()[0]
    assert sort_col.to_dict() == {'name': '1', 'sortDesc': False}
    # Assert that None is returned if the serialization is None
    assert ResultSchema.from_dict(None) is None
    # Error for duplicate column names.
    with pytest.raises(err.InvalidTemplateError):
        ResultSchema.from_dict({
            'file': 'results/analytics.json',
            'schema': [
                {'name': '0', 'label': 'col0', 'dtype': PARA_STRING},
                {'name': '1', 'label': 'col0', 'dtype': PARA_STRING}
            ]
        })
    # Error for duplicate column identifier.
    with pytest.raises(err.InvalidTemplateError):
        ResultSchema.from_dict({
            'file': 'results/analytics.json',
            'schema': [
                {'name': '0', 'label': 'col0', 'dtype': PARA_STRING},
                {'name': '0', 'label': 'col1', 'dtype': PARA_STRING}
            ]
        })
    # Error when referencing unknown column in 'Order By' clause.
    with pytest.raises(err.InvalidTemplateError):
        ResultSchema.from_dict({
            'file': 'results/analytics.json',
            'schema': [
                {'name': '0', 'label': 'col0', 'dtype': PARA_STRING},
                {'name': '1', 'label': 'col1', 'dtype': PARA_STRING}
            ],
            'orderBy': [{'name': '2', 'sortDesc': False}]
        })
    # Invalid elemtn in specification.
    ResultSchema.from_dict({
        'file': 'results/analytics.json',
        'schema': [{'name': '0', 'label': 'col0', 'dtype': PARA_STRING}],
        'notValid': 1
    }, validate=False)
    # Ensure that the validate flag is passed through.
    ResultSchema.from_dict({
        'file': 'results/analytics.json',
        'schema': [{
            'name': '0',
            'label': 'col0',
            'dtype': PARA_STRING,
            'notValid': True
        }],
        'orderBy': [{'name': '0', 'noSort': False}]
    }, validate=False)
    with pytest.raises(err.InvalidTemplateError):
        ResultSchema.from_dict({
            'file': 'results/analytics.json',
            'schema': [{'name': '0', 'label': 'col0', 'dtype': PARA_STRING}],
            'notValid': 1
        })
    with pytest.raises(err.InvalidTemplateError):
        ResultSchema.from_dict({
            'file': 'results/analytics.json',
            'schema': [{
                'name': '0',
                'label': 'col0',
                'dtype': PARA_STRING,
                'notValid': True
            }]
        })


def test_sort_column_serialization():
    """Test serailization of sore columns."""
    doc = {'name': '0', 'sortDesc': False}
    col = SortColumn.from_dict(SortColumn.from_dict(doc).to_dict())
    assert col.column_id == '0'
    assert not col.sort_desc
    # Default sort order is desc.
    doc = {'name': '0'}
    col = SortColumn.from_dict(SortColumn.from_dict(doc).to_dict())
    assert col.column_id == '0'
    assert col.sort_desc
    # Invalid column serialization.
    with pytest.raises(KeyError):
        SortColumn.from_dict({}, validate=False)
    with pytest.raises(err.InvalidTemplateError):
        SortColumn.from_dict({})
