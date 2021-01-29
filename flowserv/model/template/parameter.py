# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Collection of helper methods for parameter references in workflow templates.
"""

from typing import Dict, Optional

import re

from flowserv.model.parameter.base import Parameter
from flowserv.model.parameter.factory import ParameterDeserializer

import flowserv.error as err


"""Regular expression for template parameters."""
REGEX_PARA = r'\$\[\[(.*?)\]\]'


# -- Parameter Index ----------------------------------------------------------

class ParameterIndex(dict):
    """Index of parameter declaration. Parameters are indexed by their unique
    identifier.
    """
    @staticmethod
    def from_dict(doc: Dict, validate: Optional[bool] = True) -> Parameter:
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
            # Ensure that the 'index' property for the parameter is set. Use
            # the order of parameters in the list as the default order.
            obj['index'] = obj.get('index', index)
            # Ensure that the 'isRequired' property is set. If no default value
            # is defined the parameter is assumed to be required.
            obj['isRequired'] = obj.get(
                'isRequired',
                'defaultValue' not in obj
            )
            para = ParameterDeserializer.from_dict(obj, validate=validate)
            if para.name in parameters:
                msg = "duplicate parameter '{}'".format(para.name)
                raise err.InvalidTemplateError(msg)
            parameters[para.name] = para
        return parameters

    def sorted(self):
        """Get list of parameter declarations sorted by ascending parameter
        index position.

        Returns
        -------
        list(flowserv.model.parameter.base.Parameter)
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

def expand_value(value, arguments, parameters):
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
    # Replace function for parameter references.
    def replace_ref(match):
        """Function to replace references to template parameters in a given
        string. Used as callback function by the regular expression substitute
        method.

        Parameters
        ----------
        match: re.MatchObject
            Regular expression match object.

        Returns
        -------
        string
        """
        ref = match.group()
        # Strip expression of parameter reference syntax.
        expr = ref[3:-2]
        pos = expr.find('?')
        if pos == -1:
            para = parameters[expr]
            # If arguments contains a value for the variable we return the
            # associated value from the dictionary. Otherwise, the default
            # value is returned or and error is raised if no default value
            # is defined for the parameter.
            if expr in arguments:
                return str(arguments[expr])
            elif para.default is not None:
                # Return the parameter default value.
                return str(para.default)
            raise err.MissingArgumentError(para.name)
        # Extract the variable name and the conditional return values.
        var = expr[:pos].strip()
        expr = expr[pos + 1:].strip()
        pos = expr.find(':')
        if pos == -1:
            eval_true = expr
            eval_false = None
        else:
            eval_true = expr[:pos].strip()
            eval_false = expr[pos + 1:].strip()
        if var not in arguments:
            raise err.MissingArgumentError(var)
        if str(arguments[var]).lower() == 'true':
            return eval_true
        else:
            return eval_false

    return re.sub(REGEX_PARA, replace_ref, value)


def get_name(value):
    """Extract the parameter name for a template parameter reference.

    Parameters
    ----------
    value: string
        String value in the workflow specification for a template parameter

    Returns
    -------
    string
    """
    end_pos = value.find('?')
    if end_pos == -1:
        end_pos = -2
    return value[3: end_pos].strip()


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
            # If the value is of type string extract all parameter references.
            for match in re.finditer(REGEX_PARA, val):
                # Extract variable name.
                parameters.add(get_name(match.group()))
        elif isinstance(val, dict):
            # Recursive call to get_parameter_references
            get_parameter_references(val, parameters=parameters)
        elif isinstance(val, list):
            for list_val in val:
                if isinstance(list_val, str):
                    # Get potential references to template parameters in
                    # list elements of type string.
                    for match in re.finditer(REGEX_PARA, list_val):
                        # Extract variable name.
                        parameters.add(get_name(match.group()))
                elif isinstance(list_val, dict):
                    # Recursive replace for dictionaries
                    get_parameter_references(list_val, parameters=parameters)
                elif isinstance(list_val, list):
                    # We currently do not support lists of lists
                    raise err.InvalidTemplateError('nested lists not allowed')
    return parameters


def get_value(value, arguments):
    """Get the result value from evaluating a parameter reference expression.
    Expects a value that satisfies the is_parameter() predicate. If the given
    expression is unconditional, e.g., $[[name]], the parameter name is the
    returned result. If the expression is conditional, e.g., $[[name ? x : y]]
    the argument value for parameter 'name' is tested for being Boolean True or
    False. Depending on the outcome of the evaluation either x or y are
    returned.

    Note that nested conditional expressions are currently not supported.

    Parameters
    ----------
    value: string
        Parameter reference string that satisifes the is_parameter() predicate.
    arguments: dict
        Dictionary of user-provided argument values for template arguments.

    Returns
    -------
    string

    Raises
    ------
    flowserv.error.MissingArgumentError
    """
    # Strip expression of parameter reference syntax.
    expr = value[3:-2]
    pos = expr.find('?')
    if pos == -1:
        # Return immediately if this is an unconditional expression.
        return expr
    # Extract the variable name and the conditional return values.
    var = expr[:pos].strip()
    expr = expr[pos + 1:].strip()
    pos = expr.find(':')
    if pos == -1:
        eval_true = expr
        eval_false = None
    else:
        eval_true = expr[:pos].strip()
        eval_false = expr[pos + 1:].strip()
    if var not in arguments:
        raise err.MissingArgumentError(var)
    if str(arguments[var]).lower() == 'true':
        return eval_true
    else:
        return eval_false


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
        obj = expand_value(spec, arguments, parameters)
    else:
        obj = spec
    return obj


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
