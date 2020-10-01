# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper methods for reading workflow template parameters."""

from flowserv.model.files.fs import FSFile
from flowserv.model.parameter.boolean import is_bool
from flowserv.model.parameter.files import InputFile, is_file
from flowserv.model.parameter.numeric import is_float, is_int
from flowserv.service.run.argument import FILE

from flowserv.scanner import Scanner


def read(parameters, scanner=None, files=None):
    """Read values for each of the template parameters using a given input
    scanner. If no scanner is given, values are read from standard input.

    The optional list of file handles is used for convenience when the user is
    asked to input the identifier of an uploaded file. It allows to display the
    identifier of available files for easy copy and paste.

    Returns a dictionary of argument values that can be passed to the workflow
    execution engine to run a parameterized workflow.

    Parameters
    ----------
    parameters: list(flowserv.model.parameter.base.ParameterBase)
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
        arguments[para.para_id] = read_parameter(para, sc, files=files)
    return arguments


def read_parameter(para, scanner, files=None):
    """Read value for a given template parameter declaration. Prompts the
    user to enter a value for the given parameter and returns the converted
    value that was entered by the user.

    Parameters
    ----------
    para: flowserv.model.parameter.base.ParameterBase
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
        print(para.prompt(), end='')
        try:
            if is_bool(para):
                return scanner.next_bool(default_value=para.default_value)
            elif is_file(para):
                # Distinguish between the case where a list of uploaded files
                # is given or not.
                if files is not None:
                    print('\nUploaded files (id, name, date)\n')
                    for file_id, name, created_at in files:
                        print('\t'.join([file_id, name, created_at]))
                    print('\nFile ID $> ', end='')
                    filename = scanner.next_string()
                else:
                    filename = scanner.next_file()
                target_path = None
                if para.target is None:
                    print('Target Path:', end='')
                    target_path = scanner.next_string()
                    if target_path == '':
                        target_path = para.default_value
                else:
                    target_path = para.target
                # The type of the returned value depends on whether the list of
                # uploaded files is given or not.
                if files is not None:
                    return FILE(file_id=filename, target=target_path)
                else:
                    return InputFile(FSFile(filename), target_path)
            elif is_float(para):
                return scanner.next_float(default_value=para.default_value)
            elif is_int(para):
                return scanner.next_int(default_value=para.default_value)
            return scanner.next_string(default_value=para.default_value)
        except ValueError as ex:
            print(ex)
