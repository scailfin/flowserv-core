# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the template arguments modules. Test parsing and validating
argument values for parameterized workflow templates.
"""

import os
import pytest

from flowserv.model.parameter.base import InputFile, TemplateParameter
from flowserv.model.template.base import WorkflowTemplate

import flowserv.model.parameter.base as pbase
import flowserv.model.parameter.declaration as pd
import flowserv.model.parameter.value as values
import flowserv.model.template.base as tmpl


DIR = os.path.dirname(os.path.realpath(__file__))
LOCAL_FILE = os.path.join(DIR, '../../.files/schema.json')


def test_flat_parse():
    """Test parsing arguments for a flat (un-nested) parameter declaration.
    """
    template = WorkflowTemplate(
        workflow_spec=dict(),
        sourcedir='dev/null',
        parameters=[
            TemplateParameter(
                pd.parameter_declaration('A', data_type=pd.DT_INTEGER)
            ),
            TemplateParameter(
                pd.parameter_declaration('B', data_type=pd.DT_BOOL)
            ),
            TemplateParameter(
                pd.parameter_declaration('C', data_type=pd.DT_DECIMAL)
            ),
            TemplateParameter(
                pd.parameter_declaration('D', data_type=pd.DT_FILE)
            ),
            TemplateParameter(
                pd.parameter_declaration(
                    identifier='E',
                    data_type=pd.DT_FILE,
                    as_const='file.txt',
                    required=False
                )
            ),
            TemplateParameter(
                pd.parameter_declaration(
                    identifier='F',
                    data_type=pd.DT_STRING,
                    required=False
                )
            )
        ]
    )
    params = template.parameters
    in_fh = InputFile(
        filename=LOCAL_FILE,
        target_path='/dev/null'
    )
    # Valid argument set
    args = values.parse_arguments(
        arguments={
            'A': 10,
            'B': True,
            'C': 12.5,
            'D': in_fh,
            'E': LOCAL_FILE,
            'F': 'ABC'
        },
        parameters=params,
        validate=True
    )
    assert len(args) == 6
    for key in params.keys():
        assert key in args
    assert isinstance(args['D'].value, InputFile)
    filename = os.path.abspath(args['D'].value.source())
    assert filename == os.path.abspath(LOCAL_FILE)
    assert args['D'].value.target() == '/dev/null'
    assert isinstance(args['E'].value, InputFile)
    # Error cases
    with pytest.raises(ValueError):
        values.parse_arguments(
            arguments={'A': 10, 'Z': 0},
            parameters=params
        )
    with pytest.raises(ValueError):
        values.parse_arguments(
            arguments={'A': 10, 'B': True},
            parameters=params
        )
    # Validate data type
    with pytest.raises(ValueError):
        values.parse_arguments(
            arguments={
                'A': '10',
                'B': True,
                'C': 12.3,
                'D': in_fh,
                'F': 'ABC'
            },
            parameters=params,
            validate=True
        )
    with pytest.raises(ValueError):
        values.parse_arguments(
            arguments={
                'A': 10,
                'B': 23,
                'C': 12.3,
                'D': in_fh,
                'F': 'ABC'
            },
            parameters=params,
            validate=True
        )
    with pytest.raises(ValueError):
        values.parse_arguments(
            arguments={
                'A': 10,
                'B': True,
                'C': '12.3',
                'D': in_fh,
                'F': 'ABC'
            },
            parameters=params,
            validate=True
        )
    with pytest.raises(ValueError):
        values.parse_arguments(
            arguments={
                'A': 10,
                'B': True,
                'C': 12.3,
                'D': 'fh',
                'F': 'ABC'
            },
            parameters=params,
            validate=True
        )
    with pytest.raises(ValueError):
        values.parse_arguments(
            arguments={'A': 10, 'B': True, 'C': 12.3, 'D': in_fh, 'F': 12},
            parameters=params,
            validate=True
        )


def test_nested_parse():
    """Test parsing arguments for a nested parameter declaration."""
    template = WorkflowTemplate.from_dict({
            tmpl.LABEL_WORKFLOW: dict(),
            tmpl.LABEL_PARAMETERS: [
                pd.parameter_declaration('A', data_type=pd.DT_INTEGER),
                pd.parameter_declaration('B', data_type=pd.DT_RECORD),
                pd.parameter_declaration(
                    identifier='C',
                    data_type=pd.DT_DECIMAL,
                    parent='B'
                ),
                pd.parameter_declaration(
                    identifier='D',
                    data_type=pd.DT_STRING,
                    parent='B',
                    required=False
                ),
                pd.parameter_declaration(
                    identifier='E',
                    data_type=pd.DT_LIST,
                    required=False
                ),
                pd.parameter_declaration(
                    identifier='F',
                    data_type=pd.DT_INTEGER,
                    parent='E'
                ),
                pd.parameter_declaration(
                    identifier='G',
                    data_type=pd.DT_DECIMAL,
                    parent='E',
                    required=False
                )
            ]
        },
        sourcedir='dev/null',
        validate=True
    )
    params = template.parameters
    # Without values for list parameters
    args = values.parse_arguments(
        arguments={'A': 10, 'B': {'C': 12.3}},
        parameters=params,
        validate=True
    )
    assert len(args) == 2
    assert not args['B'].value.get('C') is None
    assert args['B'].value.get('D') is None
    assert len(args['B'].value) == 1
    assert args['B'].value.get('C').value == 12.3
    # With list arguments
    args = values.parse_arguments(
        arguments={
            'A': 10,
            'B': {'C': 12.3, 'D': 'ABC'},
            'E': [
                {'F': 1},
                {'F': 2},
                {'F': 3, 'G': 0.9}
            ]
        },
        parameters=params,
        validate=True
    )
    assert len(args) == 3
    assert not args['B'].value.get('C') is None
    assert not args['B'].value.get('D') is None
    assert len(args['B'].value) == 2
    assert len(args['E'].value) == 3
    for arg in args['E'].value:
        if arg.get('F').value < 3:
            assert len(arg) == 1
        else:
            assert len(arg) == 2
    # Error cases
    with pytest.raises(ValueError):
        values.parse_arguments(
            arguments={'A': 10, 'B': [{'C': 12.3}]},
            parameters=params,
            validate=True
        )


def test_validate():
    """Test error cases for argument validation."""
    para_list = TemplateParameter(
        pd.parameter_declaration('E', data_type=pd.DT_LIST)
    )
    with pytest.raises(ValueError):
        values.TemplateArgument(parameter=para_list, value=1)
    para_record = TemplateParameter(
        pd.parameter_declaration('E', data_type=pd.DT_RECORD)
    )
    with pytest.raises(ValueError):
        values.TemplateArgument(parameter=para_record, value=list())
    arg = values.TemplateArgument(
        parameter=TemplateParameter(
            pd.parameter_declaration('E', data_type=pd.DT_INTEGER)
        ),
        value=1,
        validate=True
    )
    # Error for unknown data type
    arg.data_type = 'unknown'
    with pytest.raises(ValueError):
        arg.validate()
    # Error for file handle as argument value for a file parameter with
    # expected target path
    with pytest.raises(ValueError):
        values.TemplateArgument(
            parameter=TemplateParameter(
                pd.parameter_declaration(
                    'A',
                    data_type=pd.DT_FILE,
                    as_const=pbase.AS_INPUT
                )
            ),
            value=LOCAL_FILE
        )
