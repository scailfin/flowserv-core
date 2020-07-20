# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper methods for reading workflow template parameters."""

from flowserv.model.parameter.boolean import PARA_BOOL
from flowserv.model.parameter.files import InputFile, PARA_FILE
from flowserv.model.parameter.numeric import PARA_FLOAT, PARA_INT

from flowserv.scanner import Scanner

import flowserv.error as err


def read(parameters, scanner=None):
    """Read values for each of the template parameters using a given input
    scanner. If no scanner is given, values are read from standard input.

    The optional list of file handles is used for convenience when the user is
    asked to input the identifier of an uploaded file. It allows to display the
    identifier of available files for easy copy and paste.

    Returns a dictionary of argument values that can be passed to the workflow
    execution engine to run a parameterized workflow.

    Parameters
    ----------
    parameters: list(flowserv.model.parameter.base.TemplateParameter)
        List of workflow template parameter declarations
    scanner: flowserv.scanner.Scanner
        Input scanner to read parameter values
    files: list
        List of (file_id, name, timestamp) pairs

    Returns
    -------
    dict
    """
    sc = scanner if scanner is not None else Scanner()
    arguments = dict()
    for para in parameters:
        arguments[para.para_id] = read_parameter(para, sc)
    return arguments


def read_parameter(para, scanner):
    """Read value for a given template parameter declaration. Prompts the
    user to enter a value for the given parameter and returns the converted
    value that was entered by the user.

    Parameters
    ----------
    para: flowserv.model.parameter.TemplateParameter
        Workflow template parameter declaration
    scanner: flowserv.scanner.Scanner
        Input scanner.

    Returns
    -------
    bool or float or int or string or tuple(string, string)
    """
    while True:
        print(para.prompt(), end='')
        try:
            if para.type_id == PARA_BOOL:
                return scanner.next_bool(default_value=para.default_value)
            elif para.type_id == PARA_FILE:
                filename = scanner.next_file()
                target_path = None
                if para.target is None:
                    print('Target Path:', end='')
                    target_path = scanner.next_string()
                    if target_path == '':
                        target_path = para.default_value
                else:
                    target_path = para.target
                return InputFile(filename, target_path)
            elif para.type_id == PARA_FLOAT:
                return scanner.next_float(default_value=para.default_value)
            elif para.type_id == PARA_INT:
                return scanner.next_int(default_value=para.default_value)
            return scanner.next_string(default_value=para.default_value)
        except (ValueError, err.UnknownFileError) as ex:
            print(ex)
