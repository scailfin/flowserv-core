# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Collection of helper methods for parameter references in workflow
specifications within workflow templates.
"""

from past.builtins import basestring

import os

from flowserv.core.files import FileHandle, InputFile
from flowserv.model.parameter.value import TemplateArgument

import flowserv.core.error as err


def get_parameter_references(spec, parameters=None):
    """Get set of parameter identifier that are referenced in the given
    workflow specification. Adds parameter identifier to the given parameter
    set.

    Parameters
    ----------
    spec: dict
        Parameterized workflow specification
    parameters: set, optional
        Result set of parameter identifier

    Returns
    -------
    set

    Raises
    ------
    flowserv.core.error.InvalidTemplateError
    """
    # The new object will contain the modified workflow specification
    if parameters is None:
        parameters = set()
    for key in spec:
        val = spec[key]
        if isinstance(val, basestring):
            # If the value is of type string we test whether the string is a
            # reference to a template parameter
            if is_parameter(val):
                # Extract variable name.
                parameters.add(NAME(val))
        elif isinstance(val, dict):
            # Recursive call to get_parameter_references
            get_parameter_references(val, parameters=parameters)
        elif isinstance(val, list):
            for list_val in val:
                if isinstance(list_val, basestring):
                    # Get potential references to template parameters in
                    # list elements of type string.
                    if is_parameter(list_val):
                        # Extract variable name.
                        parameters.add(NAME(list_val))
                elif isinstance(list_val, dict):
                    # Recursive replace for dictionaries
                    get_parameter_references(list_val, parameters=parameters)
                elif isinstance(list_val, list):
                    # We currently do not support lists of lists
                    raise err.InvalidTemplateError('nested lists not supported')
    return parameters


def get_upload_files(template, basedir, files, arguments):
    """Get a list of all input files for a workflow template that need to be
    uploaded for a new workflow run. The list of files corresponds, for
    example, to the entries in the 'inputs.files' section of a REANA workflow
    specification.

    Returns a list of tuples containing the full path to the source file on
    local disk and the relative target path for the uploaded file.

    Raises errors if (i) an unknown parameter is referenced or (ii) if the type
    of a referenced parameter in the input files section is not of type file.

    Parameters
    ----------
    template: flowserv.model.template.base.WorkflowTemplate
        Workflow template containing the parameterized specification and the
        parameter declarations
    basedir: string
        Path to the base directory of the template folder containing static
        template files
    files: list(string)
        List of file references
    arguments: dict(flowserv.model.parameter.value.TemplateArgument)
        Dictionary of argument values for parameters in the template

    Returns
    -------
    list((string, string))

    Raises
    ------
    flowserv.core.error.InvalidTemplateError
    flowserv.core.error.MissingArgumentError
    flowserv.core.error.UnknownParameterError
    """
    result = list()
    for val in files:
        # Set source and target values depending on whether the list
        # entry references a template parameter or not
        if is_parameter(val):
            var = NAME(val)
            # Raise error if the type of the referenced parameter is
            # not file
            para = template.get_parameter(var)
            if not para.is_file():
                msg = "expected file parameter for '{}'"
                raise err.InvalidTemplateError(msg.format(var))
            arg = arguments.get(var)
            if arg is None:
                if para.default_value is None:
                    raise err.MissingArgumentError(var)
                else:
                    # Set argument to file handle using the default value
                    # (assuming that the default points to a file in the
                    # template base directory).
                    if para.has_constant() and not para.as_input():
                        target_path = para.get_constant()
                    else:
                        target_path = para.default_value
                    arg = TemplateArgument(
                        parameter=para,
                        value=InputFile(
                            f_handle=FileHandle(
                                filename=os.path.join(
                                    basedir,
                                    para.default_value
                                )
                            ),
                            target_path=target_path
                        )
                    )
            # Get path to source file and the target path from the input
            # file handle
            source = arg.value.source()
            target = arg.value.target()
        else:
            source = os.path.join(basedir, val)
            target = val
        # Upload source file
        result.append((source, target))
    return result


def is_parameter(value):
    """Returns True if the given value is a reference to a template parameter.

    Parameters
    ----------
    value: string
        String value in the workflow specification for a template parameter

    Returns
    -------
    bool
    """
    # Check if the value matches the template parameter reference pattern
    return value.startswith('$[[') and value.endswith(']]')


def replace_args(spec, arguments, parameters):
    """Replace template parameter references in the workflow specification
    with their respective values in the argument dictionary or their
    defined default value. The type of the result is depending on the type
    of the spec object

    Returns a modified dictionary.

    Parameters
    ----------
    spec: any
        Parameterized workflow specification
    arguments: dict(flowserv.model.parameter.value.TemplateArgument)
        Dictionary that associates template parameter identifiers with
        argument values
    parameters: dict(flowserv.model.parameter.base.TemplateParameter)
        Dictionary of parameter declarations

    Returns
    -------
    type(spec)

    Raises
    ------
    flowserv.core.error.InvalidTemplateError
    flowserv.core.error.MissingArgumentError
    """
    if isinstance(spec, dict):
        # The new object will contain the modified workflow specification
        obj = dict()
        for key in spec:
            obj[key] = replace_args(spec[key], arguments, parameters)
    elif isinstance(spec, list):
        obj = list()
        for val in spec:
            if isinstance(val, list):
                # We currently do not support lists of lists
                raise err.InvalidTemplateError('nested lists not supported')
            obj.append(replace_args(val, arguments, parameters))
    elif isinstance(spec, basestring):
        obj = replace_value(spec, arguments, parameters)
    else:
        obj = spec
    return obj


def replace_value(value, arguments, parameters):
    """Test whether the string is a reference to a template parameter and (if
    True) replace the value with the given argument or default value.

    In the current implementation template parameters are referenced using
    $[[..]] syntax.

    Parameters
    ----------
    value: string
        String value in the workflow specification for a template parameter
    arguments: dict(flowserv.model.parameter.value.TemplateArgument)
        Dictionary that associates template parameter identifiers with
        argument values
    parameters: dict(flowserv.model.parameter.base.TemplateParameter)
        Dictionary of parameter declarations

    Returns
    -------
    string

    Raises
    ------
    flowserv.core.error.MissingArgumentError
    """
    # Check if the value matches the template parameter reference pattern
    if is_parameter(value):
        # Extract variable name.
        var = NAME(value)
        para = parameters[var]
        # If the parameter has a constant value defined use that value as the
        # replacement
        if para.has_constant():
            return para.get_constant()
        # If arguments contains a value for the variable we return the
        # associated value from the dictionary. Note that there is a special
        # treatment for file arguments. If case of file arguments the
        # dictionary value is expected to be a file handle. In this case we
        # return the file name as the argument value.
        if var in arguments:
            arg = arguments[var]
            if para.is_file():
                return arg.value.target()
            else:
                return arg.value
        elif para.default_value is not None:
            # Return the parameter default value
            return para.default_value
        else:
            raise err.MissingArgumentError(para.identifier)
    else:
        return value


# -- Helper functions to extract and generate parameter names -----------------

def NAME(value):
    """Extract the parameter name for a template parameter reference.

    Parameters
    ----------
    value: string
        String value in the workflow specification for a template parameter

    Returns
    -------
    string
    """
    return value[3:-2]


def VARIABLE(name):
    """Get string representation containing the reference to a variable with
    given name. This string is intended to be used as a template parameter
    reference within workflow specifications in workflow templates.

    Parameters
    ----------
    name: string
        Template parameter name

    Returns
    -------
    string
    """
    return '$[[{}]]'.format(name)
