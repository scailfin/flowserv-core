# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for reading arguments for serial workflow templates."""

from flowserv.model.parameter.boolean import BoolParameter
from flowserv.model.parameter.files import FileParameter
from flowserv.model.parameter.numeric import (
    NumericParameter, PARA_FLOAT, PARA_INT
)
from flowserv.model.parameter.string import StringParameter
from flowserv.scanner import Scanner, ListReader

import flowserv.cli.parameter as cli


def test_read_boolean_parameters():
    """Test reading lists of boolean parameters."""
    parameters = [
        BoolParameter(para_id='A', name='A', index=0),
        BoolParameter(para_id='B', name='B', index=1, default_value=False),
        BoolParameter(para_id='C', name='C', index=2, default_value=False),
    ]
    sc = Scanner(reader=ListReader(['true', 'xyz', '', 'True']))
    arguments = cli.read(parameters, sc)
    assert len(arguments) == 3
    assert arguments['A']
    assert not arguments['B']
    assert arguments['C']


def test_read_empty_list():
    """Test reading empty lists of parameter declarations."""
    assert cli.read([]) == dict()


def test_read_file_parameters(tmpdir):
    """Test reading lists of file parameters."""
    parameters = [
        FileParameter(para_id='A', name='A', index=0, target='target1'),
        FileParameter(para_id='B', name='B', index=1, default_value='target2'),
        FileParameter(para_id='C', name='C', index=2),
    ]
    sc = Scanner(reader=ListReader([
        'file1',
        tmpdir,
        tmpdir,
        '',
        tmpdir,
        'target3'
    ]))
    arguments = cli.read(parameters, sc)
    assert len(arguments) == 3
    assert arguments['A'].source() == tmpdir
    assert arguments['A'].target() == 'target1'
    assert arguments['B'].source() == tmpdir
    assert arguments['B'].target() == 'target2'
    assert arguments['C'].source() == tmpdir
    assert arguments['C'].target() == 'target3'


def test_read_numeric_parameters():
    """Test reading lists of numeric parameters."""
    parameters = [
        NumericParameter(para_id='A', type_id=PARA_INT, name='A', index=0),
        NumericParameter(
            para_id='B',
            type_id=PARA_INT,
            name='B',
            index=1,
            default_value=10
        ),
        NumericParameter(para_id='C', type_id=PARA_FLOAT, name='C', index=2),
        NumericParameter(
            para_id='D',
            type_id=PARA_FLOAT,
            name='D',
            index=3,
            default_value=1.23
        )
    ]
    sc = Scanner(reader=ListReader(['1', 'xyz', '', 'True', '3.4', '']))
    arguments = cli.read(parameters, sc)
    assert len(arguments) == 4
    assert arguments['A'] == 1
    assert arguments['B'] == 10
    assert arguments['C'] == 3.4
    assert arguments['D'] == 1.23


def test_read_string_parameters():
    """Test reading lists of string parameters."""
    parameters = [
        StringParameter(para_id='A', name='A', index=0),
        StringParameter(para_id='B', name='B', index=1, default_value='ABC')
    ]
    sc = Scanner(reader=ListReader(['true', '']))
    arguments = cli.read(parameters, sc)
    assert len(arguments) == 2
    assert arguments['A'] == 'true'
    assert arguments['B'] == 'ABC'
