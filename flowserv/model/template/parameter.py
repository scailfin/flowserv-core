# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Collection of helper methods for parameter references in workflow templates.
"""

from flowserv.model.parameter.base import TYPE
from flowserv.model.parameter.boolean import BoolParameter, PARA_BOOL
from flowserv.model.parameter.enum import EnumParameter, PARA_ENUM
from flowserv.model.parameter.files import FileParameter, PARA_FILE
from flowserv.model.parameter.numeric import NumericParameter
from flowserv.model.parameter.numeric import PARA_FLOAT, PARA_INT
from flowserv.model.parameter.string import StringParameter, PARA_STRING

import flowserv.error as err


# -- Parameter Index ----------------------------------------------------------

"""Dictionary of known parameter types. New types have to be added here."""
PARAMETER_TYPES = {
    PARA_BOOL: BoolParameter,
    PARA_ENUM: EnumParameter,
    PARA_FILE: FileParameter,
    PARA_FLOAT: NumericParameter,
    PARA_INT: NumericParameter,
    PARA_STRING: StringParameter
}


class ParameterIndex(dict):
    """Index of parameter declaration. Parameters are indexed by their unique
    identifier.
    """
    @staticmethod
    def from_dict(doc, validate=True):
        """Create a parameter index from a dictionary serialization. Expects a
        list of dictionaries, each being a serialized parameter declaration.

        Raises an error if parameter indices are not unique.

        Parameters
        ----------
        doc: list
            List of serialized parameter declarations.
        validate: bool, default=True
            Validate dictionary serializations if True.

        Returns
        -------
        flowserv.model.template.base.ParameterIndex
        """
        parameters = ParameterIndex()
        for index, obj in enumerate(doc):
            try:
                cls = PARAMETER_TYPES[obj[TYPE]]
            except KeyError as ex:
                msg = "missing '{}' for {}"
                raise err.InvalidTemplateError(msg.format(str(ex), obj))
            # Ensure that the 'index' property for the parameter is set. Use
            # the order of parameters in the list as the default order.
            obj['index'] = obj.get('index', index)
            # Ensure that the 'isRequired' property is set. If no default value
            # is defined the parameter is assumed to be required.
            obj['isRequired'] = obj.get(
                'isRequired',
                'defaultValue' not in obj
            )
            para = cls.from_dict(obj, validate=validate)
            if para.para_id in parameters:
                msg = "duplicate parameter '{}'".format(para.para_id)
                raise err.InvalidTemplateError(msg)
            parameters[para.para_id] = para
        return parameters

    def sorted(self):
        """Get list of parameter declarations sorted by ascending parameter
        index position.

        Returns
        -------
        list(flowserv.model.parameter.base.ParameterBase)
        """
        parameters = list(self.values())
        return sorted(parameters, key=lambda p: p.index)

    def to_dict(self):
        """Get dictionary serialization for the parameter declarations.

        Returns
        -------
        list
        """
        return [p.to_dict() for p in self.values()]


# -- Helper functions to extract and generate parameter names -----------------

def get_parameter_references(spec, parameters=None):
    """Get set of parameter identifier that are referenced in the given
    workflow specification. Adds parameter identifier to the given parameter
    set.

    Parameters
    ----------
    spec: dict
        Parameterized workflow specification.
    parameters: set, optional
        Result set of referenced parameter identifier.

    Returns
    -------
    set

    Raises
    ------
    flowserv.error.InvalidTemplateError
    """
    # Set of referenced parameter identifier.
    if parameters is None:
        parameters = set()
    for key in spec:
        val = spec[key]
        if isinstance(val, str):
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
                if isinstance(list_val, str):
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
                    raise err.InvalidTemplateError('nested lists not allowed')
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
    of the spec object.

    Parameters
    ----------
    spec: any
        Parameterized workflow specification.
    arguments: dict
        Dictionary that associates template parameter identifiers with
        argument values.
    parameters: flowserv.model.template.parameter.ParameterIndex
        Dictionary of parameter declarations.

    Returns
    -------
    type(spec)

    Raises
    ------
    flowserv.error.InvalidTemplateError
    flowserv.error.MissingArgumentError
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
    elif isinstance(spec, str):
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
    arguments: dict
        Dictionary that associates template parameter identifiers with
        argument values
    parameters: flowserv.model.template.parameter.ParameterIndex
        Dictionary of parameter declarations

    Returns
    -------
    string

    Raises
    ------
    flowserv.error.MissingArgumentError
    """
    # Check if the value matches the template parameter reference pattern
    if is_parameter(value):
        # Extract variable name.
        var = NAME(value)
        para = parameters[var]
        # If arguments contains a value for the variable we return the
        # associated value from the dictionary. Otherwise, the default value
        # is returned or and error is raised if no default value is defined
        # for the parameter.
        if var in arguments:
            return str(arguments[var])
        elif para.default_value is not None:
            # Return the parameter default value
            return para.default_value
        raise err.MissingArgumentError(para.para_id)
    return value


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
