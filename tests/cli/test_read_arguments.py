# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for reading arguments for serial workflow templates."""

import pytest

from flowserv.scanner import Scanner, ListReader
from flowserv.model.parameter.base import (
    TemplateParameter, AS_INPUT, create_parameter_index
)

import flowserv.cli.parameter as cli
import flowserv.model.parameter.declaration as pd


# -- Helper functions ---------------------------------------------------------

def PARA(data_type, default_value=None, as_const=None):
    return TemplateParameter(
        pd.parameter_declaration(
            identifier='XXX',
            data_type=data_type,
            default_value=default_value,
            as_const=as_const
        )
    )


def test_read_list_error():
    """Error for parameter of type list."""
    para = PARA(pd.DT_LIST)
    with pytest.raises(ValueError):
        cli.read([para])


def test_read_parameters():
    """Test reading lists of parameters."""
    # -- Empty parameter list -------------------------------------------------
    assert cli.read([]) == dict()
    # -- List with record -----------------------------------------------------
    parameters = create_parameter_index([
        pd.parameter_declaration(
            identifier='P1',
            data_type=pd.DT_RECORD
        ),
        pd.parameter_declaration(
            identifier='P2',
            data_type=pd.DT_INTEGER,
            parent='P1'
        ),
        pd.parameter_declaration(
            identifier='P3',
            data_type=pd.DT_INTEGER
        )
    ])
    sc = Scanner(reader=ListReader(['1', '2']))
    arguments = cli.read(parameters.values(), sc)
    assert set(arguments.values()) == {1, 2}


def test_read_parameter_error():
    """Test value error when reading parameter values."""
    para = PARA(pd.DT_INTEGER)
    sc = Scanner(reader=ListReader(['ABC', '1']))
    cli.read_parameter(para, scanner=sc) == 1


@pytest.mark.parametrize(
    'para,value,result',
    [
        (PARA(pd.DT_BOOL), 'True', True),
        (PARA(pd.DT_BOOL), 'False', False),
        (PARA(pd.DT_INTEGER), '1', 1),
        (PARA(pd.DT_DECIMAL), '2.3', 2.3),
        (PARA(pd.DT_STRING), 'ABC', 'ABC'),
        (PARA(pd.DT_FILE), 'f.txt', ('f.txt', None)),
        (PARA(pd.DT_FILE, default_value='f.txt'), '', ('f.txt', None)),
        (PARA(pd.DT_FILE, as_const=AS_INPUT), ['A', 'f.txt'], ('A', 'f.txt')),
        (PARA(pd.DT_FILE, default_value='f.txt', as_const=AS_INPUT), ['A', ''], ('A', 'f.txt'))  # noqa: E501
    ]
)
def test_read_parameter_value(para, value, result):
    """Test reading values for individual parameter types."""
    if not isinstance(value, list):
        value = [value]
    sc = Scanner(reader=ListReader(value))
    assert cli.read_parameter(para, scanner=sc) == result


@pytest.mark.parametrize('files', [None, [], [('A', 'B', 'C')]])
def test_read_file_parameter(files):
    """Test reading file parameter with various."""
    para = PARA(pd.DT_FILE)
    sc = Scanner(reader=ListReader(['ABC']))
    cli.read_parameter(para, scanner=sc, files=files) == 'ABC'
