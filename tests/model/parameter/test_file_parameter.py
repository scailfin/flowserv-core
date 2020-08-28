# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for file parameter declarations."""

import os
import pytest

from flowserv.model.parameter.files import FileParameter, PARA_FILE

import flowserv.error as err


def test_invalid_serialization():
    """Test errors for invalid serializations."""
    FileParameter.from_dict({
        'id': '0000',
        'dtype': PARA_FILE,
        'index': 0,
        'name': 'Input file',
        'isRequired': True,
        'targetPath': 'data/names.txt'
    }, validate=False)
    with pytest.raises(err.InvalidParameterError):
        FileParameter.from_dict({
            'id': '0000',
            'dtype': PARA_FILE,
            'index': 0,
            'name': 'Input file',
            'isRequired': True,
            'targetPath': 'data/names.txt'
        })
    with pytest.raises(ValueError):
        FileParameter.from_dict({
            'id': '0000',
            'dtype': 'string',
            'index': 0,
            'name': 'Name',
            'isRequired': True,
            'target': 'data/names.txt'
        })


def test_file_parameter_from_dict():
    """Test generating a file parameter declaration from a dictionary
    serialization.
    """
    para = FileParameter.from_dict(
        FileParameter.to_dict(
            FileParameter.from_dict({
                'id': '0000',
                'dtype': PARA_FILE,
                'name': 'Names',
                'index': 2,
                'description': 'List of names',
                'defaultValue': 'data/default_names.txt',
                'isRequired': False,
                'module': 'inputs',
                'target': 'data/names.txt'
            })
        )
    )
    assert para.para_id == '0000'
    assert para.type_id == PARA_FILE
    assert para.name == 'Names'
    assert para.index == 2
    assert para.description == 'List of names'
    assert para.default_value == 'data/default_names.txt'
    assert not para.is_required
    assert para.module_id == 'inputs'
    assert para.target == 'data/names.txt'


def test_file_parameter_value(tmpdir):
    """Test getting argument value for a file parameter."""
    filename = os.path.abspath(tmpdir)
    # -- Parameter target value
    para = FileParameter('0000', 'name', 0, target='data/names.txt')
    file = para.to_argument(filename)
    assert file.source() == filename
    assert file.target() == 'data/names.txt'
    assert str(file) == file.target()
    # -- Parameter default value
    para = FileParameter('0000', 'name', 0, default_value='data/names.txt')
    file = para.to_argument(filename)
    assert file.source() == filename
    assert file.target() == 'data/names.txt'
    assert str(file) == file.target()
    # -- Error for missing target
    para = FileParameter('0000', 'name', 0)
    with pytest.raises(err.InvalidArgumentError):
        para.to_argument(filename)
    # -- Missing file without error
    para = FileParameter('0000', 'name', 0, target='data/names.txt')
    filename = os.path.join(filename, 'missing.txt')
    file = para.to_argument(filename, exists=False)
    assert file.source() == filename
    assert file.target() == 'data/names.txt'
    # Missing file with error
    with pytest.raises(err.UnknownFileError):
        para.to_argument(filename)
    # Invalid argument.
    with pytest.raises(err.InvalidArgumentError):
        para.to_argument(value={'A': 1}, target='/dev/null')
