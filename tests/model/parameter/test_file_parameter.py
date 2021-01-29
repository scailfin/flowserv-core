# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for file parameter declarations."""

import os
import pytest

from flowserv.model.files.fs import FSFile
from flowserv.model.parameter.files import File, PARA_FILE

import flowserv.error as err


def test_invalid_serialization():
    """Test errors for invalid serializations."""
    File.from_dict({
        'name': '0000',
        'dtype': PARA_FILE,
        'index': 0,
        'label': 'Input file',
        'isRequired': True,
        'targetPath': 'data/names.txt'
    }, validate=False)
    with pytest.raises(err.InvalidParameterError):
        File.from_dict({
            'name': '0000',
            'dtype': PARA_FILE,
            'index': 0,
            'label': 'Input file',
            'isRequired': True,
            'targetPath': 'data/names.txt'
        })
    with pytest.raises(ValueError):
        File.from_dict({
            'name': '0000',
            'dtype': 'string',
            'index': 0,
            'label': 'Name',
            'isRequired': True,
            'target': 'data/names.txt'
        })


def test_file_parameter_from_dict():
    """Test generating a file parameter declaration from a dictionary
    serialization.
    """
    para = File.from_dict(
        File.to_dict(
            File.from_dict({
                'name': '0000',
                'dtype': PARA_FILE,
                'label': 'Names',
                'index': 2,
                'help': 'List of names',
                'defaultValue': 'data/default_names.txt',
                'isRequired': False,
                'group': 'inputs',
                'target': 'data/names.txt'
            })
        )
    )
    assert para.is_file()
    assert para.name == '0000'
    assert para.dtype == PARA_FILE
    assert para.label == 'Names'
    assert para.index == 2
    assert para.help == 'List of names'
    assert para.default == 'data/default_names.txt'
    assert not para.required
    assert para.group == 'inputs'
    assert para.target == 'data/names.txt'


def test_file_parameter_value(tmpdir):
    """Test getting argument value for a file parameter."""
    filename = os.path.abspath(tmpdir)
    # -- Parameter target value
    para = File('0000', 0, target='data/names.txt')
    file = para.cast(FSFile(filename))
    assert file.source().filename == filename
    assert file.target() == 'data/names.txt'
    assert str(file) == file.target()
    # -- Parameter default value
    para = File('0000', 0, default='data/names.txt')
    file = para.cast(FSFile(filename))
    assert file.source().filename == filename
    assert file.target() == 'data/names.txt'
    assert str(file) == file.target()
    # -- Error for missing target
    para = File('0000', 0)
    with pytest.raises(err.InvalidArgumentError):
        para.cast(filename)
    # -- Missing file without error
    para = File('0000', 0, target='data/names.txt')
    filename = os.path.join(filename, 'missing.txt')
    file = para.cast(FSFile(filename))
    assert file.source().filename == filename
    assert file.target() == 'data/names.txt'
    # Invalid argument.
    with pytest.raises(err.InvalidArgumentError):
        para.cast(value={'A': 1}, target='/dev/null')
