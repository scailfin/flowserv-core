# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper methods for reading workflow template parameters."""

from __future__ import print_function

from flowserv.core.scanner import Scanner


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
    parameters: list(flowserv.model.parameter.base.TemplateParameter)
        List of workflow template parameter declarations
    scanner: flowserv.core.scanner.Scanner
        Input scanner to read parameter values
    files: list()
        List of (idenifier, name ) pairs

    Returns
    -------
    dict
    """
    sc = scanner if scanner is not None else Scanner()
    arguments = dict()
    for para in parameters:
        # Skip nested parameter
        if para.parent is not None:
            continue
        if para.is_list():
            raise ValueError('lists are not supported yet')
        elif para.is_record():
            # A record can only appear once and all record children have
            # global unique identifier. Thus, we can add values for each
            # of the children directly to the arguments dictionary
            for child in para.children:
                val = read_parameter(child, sc, prompt_prefix='  ')
                if val is not None:
                    arguments[child.identifier] = val
        else:
            val = read_parameter(para, sc, files=files)
            if val is not None:
                arguments[para.identifier] = val
    return arguments


def read_parameter(para, scanner, prompt_prefix='', files=None):
    """Read value for a given template parameter declaration. Prompts the
    user to enter a value for the given parameter and returns the converted
    value that was entered by the user.

    Parameters
    ----------
    para: flowserv.model.parameter.TemplateParameter
        Workflow template parameter declaration
    scanner: flowserv.core.scanner.Scanner
    prompt_prefix: string, optional
    files: list(flowserv.core.files.FileDescriptor)
        List of file descriptors

    Returns
    -------
    bool or float or int or string or tuple(string, string)
    """
    while True:
        print(prompt_prefix + para.prompt(), end='')
        try:
            if para.is_bool():
                return scanner.next_bool(default_value=para.default_value)
            elif para.is_file():
                # The scanner is primarily intended for the client command line
                # interface to the web API. On the client side, when submitting
                # a run with file parameters we only need to read the identifier
                # of a previously uploaded file. If the optional target path is
                # defined as 'variable' we also need to read the target path.
                # Therefore, the result here is a tuple of filename and target
                # path. The target path may be None.
                if files is not None:
                    print('\n\nAvailable files')
                    print('---------------')
                    for fh in files:
                        print('{}\t{} ({})'.format(
                            fh.identifier,
                            fh.name,
                            fh.created_at_local_time())
                        )
                    print('\n{}: '.format(para.name), end='')
                filename = scanner.next_file(default_value=para.default_value)
                target_path = None
                if para.has_constant() and para.as_input():
                    print('Target Path:', end='')
                    target_path = scanner.next_string()
                    if target_path == '':
                        target_path = para.default_value
                return (filename, target_path)
            elif para.is_float():
                return scanner.next_float(default_value=para.default_value)
            elif para.is_int():
                return scanner.next_int(default_value=para.default_value)
            else:
                return scanner.next_string(default_value=para.default_value)
        except ValueError as ex:
            print(ex)
