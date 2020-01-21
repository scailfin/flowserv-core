# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Collection of helper methods for workflow templates."""

from past.builtins import basestring

import flowserv.core.error as err


def get_parameter_name(value):
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
                parameters.add(get_parameter_name(val))
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
                        parameters.add(get_parameter_name(list_val))
                elif isinstance(list_val, dict):
                    # Recursive replace for dictionaries
                    get_parameter_references(list_val, parameters=parameters)
                elif isinstance(list_val, list):
                    # We currently do not support lists of lists
                    raise err.InvalidTemplateError('nested lists not supported')
    return parameters


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
    arguments: dict(flowserv.model.template.parameter.value.TemplateArgument)
        Dictionary that associates template parameter identifiers with
        argument values
    parameters: dict(flowserv.model.template.parameter.base.TemplateParameter)
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
    arguments: dict(flowserv.model.template.parameter.value.TemplateArgument)
        Dictionary that associates template parameter identifiers with
        argument values
    parameters: dict(flowserv.model.template.parameter.base.TemplateParameter)
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
        var = get_parameter_name(value)
        para = parameters[var]
        # If the parameter has a constant value defined use that value as the
        # replacement
        if para.has_constant():
            return para.get_constant()
        # If arguments contains a value for the variable we return the
        # associated value from the dictionary. Note that there is a special
        # treatment for file arguments. If case of file arguments the dictionary
        # value is expected to be a file handle. In this case we return the
        # file name as the argument value.
        if var in arguments:
            arg = arguments[var]
            if para.is_file():
                return arg.value.target()
            else:
                return arg.value
        elif not para.default_value is None:
            # Return the parameter default value
            return para.default_value
        else:
            raise err.MissingArgumentError(para.identifier)
    else:
        return value
