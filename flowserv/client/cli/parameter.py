# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper methods for reading workflow template parameters."""

from typing import Any, Dict, List, Optional, Tuple

from flowserv.client.cli.table import ResultTable
from flowserv.model.parameter.base import Parameter, PARA_STRING
from flowserv.model.parameter.files import InputFile
from flowserv.scanner import Scanner
from flowserv.service.run.argument import serialize_fh
from flowserv.volume.fs import FSFile


def read(
    parameters: List[Parameter], scanner: Optional[Scanner] = None,
    files: Optional[Tuple[str, str, str]] = None
) -> Dict:
    """Read values for each of the template parameters using a given input
    scanner. If no scanner is given, values are read from standard input.

    The optional list of file handles is used for convenience when the user is
    asked to input the identifier of an uploaded file. It allows to display the
    identifier of available files for easy copy and paste.

    Returns a dictionary of argument values that can be passed to the workflow
    execution engine to run a parameterized workflow.

    Parameters
    ----------
    parameters: list(flowserv.model.parameter.base.Parameter)
        List of workflow template parameter declarations
    scanner: flowserv.scanner.Scanner
        Input scanner to read parameter values
    files: list, default=None
        List of tuples representing uploaded files. Each tuple has three
        elements: file_id, name, timestamp.

    Returns
    -------
    dict
    """
    sc = scanner if scanner is not None else Scanner()
    arguments = dict()
    for para in parameters:
        arguments[para.name] = read_parameter(para, sc, files=files)
    return arguments


def read_parameter(
    para: Parameter, scanner: Scanner, files: Optional[Tuple[str, str, str]] = None
) -> Any:
    """Read value for a given template parameter declaration. Prompts the
    user to enter a value for the given parameter and returns the converted
    value that was entered by the user.

    Parameters
    ----------
    para: flowserv.model.parameter.base.Parameter
        Workflow template parameter declaration
    scanner: flowserv.scanner.Scanner
        Input scanner.
    files: list, default=None
        List of tuples representing uploaded files. Each tuple has three
        elements: file_id, name, timestamp.

    Returns
    -------
    bool or float or int or string or tuple(string, string)
    """
    while True:
        if not para.is_file():
            print(para.prompt(), end='')
        try:
            if para.is_bool():
                return scanner.next_bool(default_value=para.default)
            elif para.is_file():
                return read_file(para=para, scanner=scanner, files=files)
            elif para.is_float():
                return scanner.next_float(default_value=para.default)
            elif para.is_int():
                return scanner.next_int(default_value=para.default)
            return scanner.next_string(default_value=para.default)
        except ValueError as ex:
            print(ex)


def read_file(
    para: Parameter, scanner: Scanner, files: Optional[Tuple[str, str, str]] = None
):
    """Read value for a file parameter.

    Parameters
    ----------
    para: flowserv.model.parameter.base.Parameter
        Workflow template parameter declaration
    scanner: flowserv.scanner.Scanner
        Input scanner.
    files: list, default=None
        List of tuples representing uploaded files. Each tuple has three
        elements: file_id, name, timestamp.
    """
    # Distinguish between the case where a list of uploaded files
    # is given or not.
    if files is not None:
        print('\nSelect file identifier from uploaded files:\n')
        table = ResultTable(
            headline=['ID', 'Name', 'Created at'],
            types=[PARA_STRING] * 3
        )
        for file_id, name, created_at in files:
            table.add([file_id, name, created_at])
        for line in table.format():
            print(line)
        print('\n{}'.format(para.prompt()), end='')
        filename = scanner.next_string()
    else:
        filename = scanner.next_file()
    target_path = None
    if para.target is None:
        print('Target Path:', end='')
        target_path = scanner.next_string()
        if target_path == '':
            target_path = para.default
    else:
        target_path = para.target
    # The type of the returned value depends on whether the list of
    # uploaded files is given or not.
    if files is not None:
        return serialize_fh(file_id=filename, target=target_path)
    else:
        return InputFile(FSFile(filename, raise_error=False), target_path)
