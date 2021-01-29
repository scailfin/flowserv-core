# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for reading arguments for serial workflow templates."""

from flowserv.model.parameter.boolean import Bool
from flowserv.model.parameter.files import File
from flowserv.model.parameter.numeric import Int, Float
from flowserv.model.parameter.string import String
from flowserv.scanner import Scanner, ListReader
from flowserv.service.run.argument import serialize_fh

import flowserv.client.cli.parameter as cli


def test_read_boolean_parameters():
    """Test reading lists of boolean parameters."""
    parameters = [
        Bool(name='A', index=0),
        Bool(name='B', index=1, default=False),
        Bool(name='C', index=2, default=False),
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
        File(name='A', index=0, target='target1'),
        File(name='B', index=1, default='target2'),
        File(name='C', index=2),
    ]
    sc = Scanner(reader=ListReader([
        tmpdir,
        tmpdir,
        '',
        tmpdir,
        'target3'
    ]))
    arguments = cli.read(parameters, sc)
    assert len(arguments) == 3
    assert arguments['A'].source().filename == tmpdir
    assert arguments['A'].target() == 'target1'
    assert arguments['B'].source().filename == tmpdir
    assert arguments['B'].target() == 'target2'
    assert arguments['C'].source().filename == tmpdir
    assert arguments['C'].target() == 'target3'


def test_read_file_parameter_with_uploads(tmpdir):
    """Test reading a file parameter with a given list of upload files."""
    parameters = [
        File(name='A', index=0, target='target1')
    ]
    sc = Scanner(reader=ListReader(['f1']))
    arguments = cli.read(parameters, sc, files=[('f1', 'F', '123')])
    assert len(arguments) == 1
    assert arguments['A'] == serialize_fh('f1', target='target1')


def test_read_numeric_parameters():
    """Test reading lists of numeric parameters."""
    parameters = [
        Int(name='A', index=0),
        Int(name='B', index=1, default=10),
        Float(name='C', index=2),
        Float(name='D', index=3, default=1.23)
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
        String(name='A', index=0),
        String(name='B', index=1, default='ABC')
    ]
    sc = Scanner(reader=ListReader(['true', '']))
    arguments = cli.read(parameters, sc)
    assert len(arguments) == 2
    assert arguments['A'] == 'true'
    assert arguments['B'] == 'ABC'
