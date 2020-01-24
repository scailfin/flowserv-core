# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for functionality of the template base module."""

import os
import pytest

from flowserv.core.files import FileHandle
from flowserv.model.parameter.base import TemplateParameter
from flowserv.model.parameter.value import TemplateArgument
from flowserv.model.template.base import WorkflowTemplate

import flowserv.core.error as err
import flowserv.core.util as util
import flowserv.model.parameter.base as pb
import flowserv.model.parameter.declaration as pd
import flowserv.model.parameter.value as pr
import flowserv.model.template.base as tmpl
import flowserv.model.template.schema as schema
import flowserv.model.template.util as tmplutil


DIR = os.path.dirname(os.path.realpath(__file__))
PREDICTOR_YAML_FILE = os.path.join(DIR, '../../.files/benchmark/predictor.yaml')
TEMPLATE_JSON_FILE = os.path.join(DIR, '../../.files/template/template.json')
TEMPLATE_YAML_FILE = os.path.join(DIR, '../../.files/template/template.yaml')
TOPTAGGER_YAML_FILE = os.path.join(DIR, '../../.files/benchmark/top-tagger.yaml')
CODE_FILE = os.path.join(DIR, '../../.files/template/code/helloworld.py')
DATA_FILE = os.path.join(DIR, '../../.files/template/inputs/short-names.txt')
# Benchmark templates with errors
BENCHMARK_DIR = os.path.join(DIR, '../../.files/benchmark')
BENCHMARK_ERR_1 = 'ERROR1.json'
BENCHMARK_ERR_2 = 'ERROR2.json'
BENCHMARK_ERR_3 = 'ERROR3.json'
BENCHMARK_ERR_4 = 'ERROR4.json'


def test_benchmark():
    """Test reading benchmark template with order by clause and schema
    path elements.
    """
    doc = util.read_object(filename=PREDICTOR_YAML_FILE)
    template = WorkflowTemplate.from_dict(doc, 'dev/null')
    assert len(template.parameters) == 1
    assert len(template.get_schema().order_by) == 2
    schema = template.get_schema()
    assert len(schema.columns) == 2
    for col in schema.columns:
        assert col.path is None
        assert len(col.jpath()) == 1
        assert col.jpath()[0] == col.identifier
    # Complete workflow template
    doc = util.read_object(filename=TOPTAGGER_YAML_FILE)
    template = WorkflowTemplate.from_dict(doc, 'dev/null')
    assert len(template.parameters) == 4
    schema = template.get_schema()
    assert len(schema.columns) == 3
    for col in schema.columns:
        assert col.path is not None
        assert len(col.jpath()) == 2
    assert len(template.modules) == 2
    assert template.postproc_spec is not None
    template = WorkflowTemplate.from_dict(template.to_dict(), 'dev/null')
    assert len(template.parameters) == 4
    schema = template.get_schema()
    assert len(schema.columns) == 3
    for col in schema.columns:
        assert col.path is not None
        assert len(col.jpath()) == 2
    assert len(template.modules) == 2
    assert template.modules[0].identifier == 'preproc'
    assert template.modules[0].name == 'Pre-Processing Step'
    assert template.modules[1].identifier == 'eval'
    assert template.modules[1].name == 'ML Evaluation Step'
    assert template.postproc_spec is not None


def test_file_args():
    """Test the replace_args function for file parameters."""
    spec = {
        'input': [
            '$[[codeFile]]'
        ]
    }
    parameters = pb.create_parameter_index([
        {
            'id': 'codeFile',
            'datatype': 'file',
            'defaultValue': 'src/helloworld.py',
            'as': 'code/helloworld.py'
        }
    ])
    # Test default values (no arguments)
    wf = tmplutil.replace_args(
        spec=spec,
        parameters=parameters,
        arguments=dict()
    )
    assert wf['input'] == ['code/helloworld.py']
    # Test default values (with arguments)
    wf = tmplutil.replace_args(
        spec=spec,
        parameters=parameters,
        arguments=pr.parse_arguments(
            arguments={'codeFile': FileHandle(filename=CODE_FILE)},
            parameters=parameters
        )
    )
    assert wf['input'] == ['code/helloworld.py']
    # Test file parameters without constant value
    parameters = pb.create_parameter_index([
        {
            'id': 'codeFile',
            'datatype': 'file',
            'defaultValue': 'src/helloworld.py'
        }
    ])
    # Test default values (no arguments)
    wf = tmplutil.replace_args(
        spec=spec,
        parameters=parameters,
        arguments=dict()
    )
    assert wf['input'] == ['src/helloworld.py']
    wf = tmplutil.replace_args(
        spec=spec,
        parameters=parameters,
        arguments=pr.parse_arguments(
            arguments={'codeFile': FileHandle(filename=CODE_FILE)},
            parameters=parameters
        )
    )
    assert wf['input'] == ['helloworld.py']


def test_scalar_args():
    """Test the replace_args function for scalar values."""
    spec = {
        'parameters': {
            'sleeptime': '$[[sleeptime]]'
        }
    }
    parameters = pb.create_parameter_index([
        {
            'id': 'sleeptime',
            'datatype': 'int',
            'defaultValue': 10
        }
    ])
    # Test default values (no arguments)
    wf = tmplutil.replace_args(
        spec=spec,
        parameters=parameters,
        arguments=dict()
    )
    assert wf['parameters'] == {'sleeptime': 10}
    # Test default values (with arguments)
    wf = tmplutil.replace_args(
        spec=spec,
        parameters=parameters,
        arguments=pr.parse_arguments(
            arguments={'sleeptime': 5},
            parameters=parameters
        )
    )
    assert wf['parameters'] == {'sleeptime': 5}


def test_from_dict():
    """Test creating workflow template instances from dictionaries."""
    # Minimal
    template = WorkflowTemplate.from_dict(
        doc={
            tmpl.LABEL_WORKFLOW: {'A': 1, 'B': 2}
        },
        sourcedir='dev/null'
    )
    assert len(template.list_parameters()) == 0
    assert not template.has_schema()
    assert template.workflow_spec == {'A': 1, 'B': 2}
    # Error for missing workflow element
    with pytest.raises(err.InvalidTemplateError):
        WorkflowTemplate.from_dict(dict(), 'dev/null')
    with pytest.raises(err.InvalidTemplateError):
        WorkflowTemplate.from_dict(
            doc={
                tmpl.LABEL_WORKFLOW: {'A': 1, 'B': 2},
                tmpl.LABEL_RESULTS: {
                    schema.SCHEMA_COLUMNS: [{'A': 1}]
                }
            },
            sourcedir='dev/null'
        )
    # Error for non-unique parameter names
    with pytest.raises(err.InvalidTemplateError):
        WorkflowTemplate.from_dict(
            doc={
                tmpl.LABEL_WORKFLOW: {'A': 1, 'B': 2},
                tmpl.LABEL_PARAMETERS: [
                    TemplateParameter(pd.parameter_declaration('A')).to_dict(),
                    TemplateParameter(pd.parameter_declaration('B')).to_dict(),
                    TemplateParameter(pd.parameter_declaration('A')).to_dict()
                ]
            },
            sourcedir='dev/null'
        )
    # Read erroneous templates from disk
    filename = os.path.join(BENCHMARK_DIR, BENCHMARK_ERR_1)
    with pytest.raises(err.UnknownParameterError):
        WorkflowTemplate.from_dict(util.read_object(filename), '/dev/null')
    WorkflowTemplate.from_dict(
        util.read_object(filename),
        '/dev/null',
        validate=False
    )
    filename = os.path.join(BENCHMARK_DIR, BENCHMARK_ERR_2)
    with pytest.raises(err.InvalidTemplateError):
        WorkflowTemplate.from_dict(util.read_object(filename), '/dev/null')
    filename = os.path.join(BENCHMARK_DIR, BENCHMARK_ERR_3)
    with pytest.raises(err.InvalidTemplateError):
        WorkflowTemplate.from_dict(util.read_object(filename), '/dev/null')
    filename = os.path.join(BENCHMARK_DIR, BENCHMARK_ERR_4)
    with pytest.raises(err.InvalidTemplateError):
        WorkflowTemplate.from_dict(util.read_object(filename), '/dev/null')


def test_get_parameter_references():
    """Test function to get all parameter references in a workflow
    specification.
    """
    spec = {
        'input': [
            'A',
            '$[[X]]',
            {'B': {'C': '$[[Y]]', 'D': [123, '$[[Z]]']}}
        ],
        'E': {'E': 'XYZ', 'F': 23, 'G': '$[[W]]'},
        'F': '$[[U]]',
        'G': ['$[[V]]', 123]
    }
    refs = tmplutil.get_parameter_references(spec)
    assert refs == set(['U', 'V', 'W', 'X', 'Y', 'Z'])
    # If given parameter set as argument the elements in that set are part
    # of the result
    para = set(['A', 'B', 'X'])
    refs = tmplutil.get_parameter_references(spec, parameters=para)
    assert refs == set(['A', 'B', 'U', 'V', 'W', 'X', 'Y', 'Z'])
    # Error if specification contains nested lists
    with pytest.raises(err.InvalidTemplateError):
        tmplutil.get_parameter_references({
            'input': [
                'A',
                ['$[[X]]'],
                {'B': {'C': '$[[Y]]', 'D': [123, '$[[Z]]']}}
            ]
        })


def test_init():
    """Test initialization of attributes and error cases when creating
    template instances.
    """
    # Minimal
    template = WorkflowTemplate(
        workflow_spec={'A': 1, 'B': 2},
        sourcedir='dev/null'
    )
    assert len(template.list_parameters()) == 0
    assert not template.has_schema()
    assert template.workflow_spec == {'A': 1, 'B': 2}
    # Dictionary
    template = WorkflowTemplate(
        workflow_spec=dict(),
        sourcedir='dev/null',
        parameters={
            'A': TemplateParameter(pd.parameter_declaration('A')),
            'B': TemplateParameter(pd.parameter_declaration('B'))
        }
    )
    assert len(template.parameters) == 2
    assert 'A' in template.parameters
    assert 'B' in template.parameters
    # List
    template = WorkflowTemplate(
        workflow_spec=dict(),
        sourcedir='dev/null',
        parameters=[
            TemplateParameter(pd.parameter_declaration('A')),
            TemplateParameter(pd.parameter_declaration('B'))
        ]
    )
    assert len(template.parameters) == 2
    assert 'A' in template.parameters
    assert 'B' in template.parameters
    # Optional identifier is handled correctly
    template = WorkflowTemplate(
        workflow_spec=dict(),
        sourcedir='dev/null',
        parameters={
            'A': TemplateParameter(pd.parameter_declaration('A')),
            'B': TemplateParameter(pd.parameter_declaration('B'))
        }
    )
    # Error for invalid key-identifiers
    with pytest.raises(err.InvalidTemplateError):
        WorkflowTemplate(
            workflow_spec=dict(),
            sourcedir='dev/null',
            parameters={
                'A': TemplateParameter(pd.parameter_declaration('A')),
                'B': TemplateParameter(pd.parameter_declaration('B')),
                'C': TemplateParameter(pd.parameter_declaration('A'))
            }
        )
    with pytest.raises(err.InvalidTemplateError):
        WorkflowTemplate(
            workflow_spec=dict(),
            sourcedir='dev/null',
            parameters=[
                TemplateParameter(pd.parameter_declaration('A')),
                TemplateParameter(pd.parameter_declaration('B')),
                TemplateParameter(pd.parameter_declaration('A'))
            ]
        )


def test_nested_parameters():
    """Test proper nesting of parameters for DT_LIST and DT_RECORD."""
    # Create a new WorkflowTemplate with an empty workflow specification
    # and a list of six parameters (one record and one list)
    template = WorkflowTemplate.from_dict({
            tmpl.LABEL_WORKFLOW: dict(),
            tmpl.LABEL_PARAMETERS: [
                pd.parameter_declaration('A'),
                pd.parameter_declaration('B', data_type=pd.DT_RECORD),
                pd.parameter_declaration('C', parent='B'),
                pd.parameter_declaration('D', parent='B'),
                pd.parameter_declaration('E', data_type=pd.DT_LIST),
                pd.parameter_declaration('F', parent='E'),
            ]
        },
        sourcedir='dev/null',
        validate=True
    )
    # Parameters 'A', 'C', 'D', and 'F' have no children
    for key in ['A', 'C', 'D', 'F']:
        assert not template.get_parameter(key).has_children()
    # Parameter 'B' has two children 'C' and 'D'
    b = template.get_parameter('B')
    assert b.has_children()
    assert len(b.children) == 2
    assert 'C' in [p.identifier for p in b.children]
    assert 'D' in [p.identifier for p in b.children]
    # Parameter 'E' has one childr 'F'
    e = template.get_parameter('E')
    assert e.has_children()
    assert len(e.children) == 1
    assert 'F' in [p.identifier for p in e.children]


def test_serialize_template():
    """Test serialization of workflow templates."""
    template = WorkflowTemplate(
        workflow_spec=dict(),
        sourcedir='dev/null',
        parameters={
            'A': TemplateParameter(pd.parameter_declaration('A')),
            'B': TemplateParameter(pd.parameter_declaration(
                'B',
                data_type=pd.DT_LIST
            )),
            'C': TemplateParameter(pd.parameter_declaration('C', parent='B'))
        }
    )
    doc = template.to_dict()
    parameters = WorkflowTemplate.from_dict(doc, 'dev/null').parameters
    assert len(parameters) == 3
    assert 'A' in parameters
    assert 'B' in parameters
    assert len(parameters['B'].children) == 1
    template = WorkflowTemplate.from_dict(doc, 'dev/null')
    # Missing parameter list
    template = WorkflowTemplate(
        workflow_spec=dict(),
        sourcedir='dev/null'
    )
    parameters = WorkflowTemplate.from_dict(
        template.to_dict(),
        'dev/null'
    ).parameters
    assert len(parameters) == 0


def test_simple_replace():
    """Replace parameter references in simple template with argument values.
    """
    for filename in [TEMPLATE_YAML_FILE, TEMPLATE_JSON_FILE]:
        template = WorkflowTemplate.from_dict(
            util.read_object(filename),
            'dev/null'
        )
        arguments = {
            'code': TemplateArgument(
                parameter=template.get_parameter('code'),
                value=FileHandle(CODE_FILE)
            ),
            'names': TemplateArgument(
                parameter=template.get_parameter('names'),
                value=FileHandle(DATA_FILE)
            ),
            'sleeptime': TemplateArgument(
                parameter=template.get_parameter('sleeptime'),
                value=10
            )
        }
        spec = tmplutil.replace_args(
            spec=template.workflow_spec,
            arguments=arguments,
            parameters=template.parameters
        )
        assert spec['inputs']['files'][0] == 'helloworld.py'
        assert spec['inputs']['files'][1] == 'data/names.txt'
        assert spec['inputs']['parameters']['helloworld'] == 'code/helloworld.py'
        assert spec['inputs']['parameters']['inputfile'] == 'data/names.txt'
        assert spec['inputs']['parameters']['sleeptime'] == 10
        assert spec['inputs']['parameters']['waittime'] == 5
        # Error when argument is missing for parameter with no default
        # value
        del arguments['sleeptime']
        with pytest.raises(err.MissingArgumentError):
            tmplutil.replace_args(
                spec=template.workflow_spec,
                arguments=arguments,
                parameters=template.parameters
            )


def test_sort():
    """Test the sort functionality of the template list_parameters method.
    """
    # Create a new WorkflowTemplate with an empty workflow specification
    # and a list of five parameters
    template = WorkflowTemplate.from_dict({
            tmpl.LABEL_WORKFLOW: dict(),
            tmpl.LABEL_PARAMETERS: [
                pd.parameter_declaration('A', index=1),
                pd.parameter_declaration('B'),
                pd.parameter_declaration('C'),
                pd.parameter_declaration('D', index=2),
                pd.parameter_declaration('E', index=1)
            ]
        },
        sourcedir='dev/null',
        validate=True
    )
    # Get list of sorted parameter identifier from listing
    keys = [p.identifier for p in template.list_parameters()]
    assert keys == ['B', 'C', 'A', 'E', 'D']
