# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Specification for workflow template parameter declarations. This module
defines the Json schema for parameter declarations that are part of a workflow
template.

In addition, helper methods are provided for convenience to create parameter
declarations from within Python scripts.
"""

from jsonschema import validate, ValidationError

from flowserv.core.error import InvalidParameterError


# ------------------------------------------------------------------------------
# Schema
# ------------------------------------------------------------------------------

"""Labels for elements in the schema of a parameter declaration."""
LABEL_AS = 'as'
LABEL_DATATYPE = 'datatype'
LABEL_DEFAULT = 'defaultValue'
LABEL_DESCRIPTION = 'description'
LABEL_ID = 'id'
LABEL_IS_DEFAULT = 'isDefault'
LABEL_INDEX = 'index'
LABEL_MODULE = 'module'
LABEL_NAME = 'name'
LABEL_PARENT = 'parent'
LABEL_REQUIRED = 'required'
LABEL_VALUE = 'value'
LABEL_VALUES = 'values'


PARAMETER_SCHEMA = {
    'type': 'object',
    'properties': {
        LABEL_ID: {'type': 'string'},
        LABEL_NAME: {'type': 'string'},
        LABEL_DESCRIPTION: {'type': 'string'},
        LABEL_DATATYPE: {'type': 'string'},
        LABEL_PARENT: {'type': 'string'},
        LABEL_VALUES: {
            'type': 'array',
            'items': {
                'type': 'object',
                'properties': {
                    LABEL_IS_DEFAULT: {'type': 'boolean'},
                    LABEL_NAME: {'type': 'string'},
                    LABEL_VALUE: {'oneOf': [
                        {'type': 'boolean'},
                        {'type': 'string'},
                        {'type': 'number'}
                    ]}
                },
                'required': [LABEL_VALUE]
            }
        },
        LABEL_REQUIRED: {'type': 'boolean'},
        LABEL_DEFAULT: {'oneOf': [
            {'type': 'boolean'},
            {'type': 'string'},
            {'type': 'number'}
        ]},
        LABEL_INDEX: {'type': 'number'},
        LABEL_MODULE: {'type': 'string'},
        LABEL_AS: {'type': 'string'}
    },
    'required': [LABEL_ID]
}


# ------------------------------------------------------------------------------
# Data types for template parameters
# ------------------------------------------------------------------------------

"""Definition of parameter data types."""
DT_BOOL = 'bool'
DT_DECIMAL = 'decimal'
DT_FILE = 'file'
DT_INTEGER = 'int'
DT_LIST = 'list'
DT_RECORD = 'record'
DT_STRING = 'string'

DATA_TYPES = [
    DT_BOOL,
    DT_DECIMAL,
    DT_FILE,
    DT_INTEGER,
    DT_LIST,
    DT_RECORD,
    DT_STRING
]


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def enum_value(value, text=None, is_default=False):
    """Create dictionary representing a value in an enumeration of possible
    values for a parameter.

    Parameters
    ----------
    value: int, float or string
        Enumeration value
    text: string, optional
        Text representation for the value in an front-end form
    is_default: bool, optional
        Flag indicating whether this is the default value for the list

    Returns
    -------
    dict
    """
    obj = {LABEL_VALUE: value, LABEL_IS_DEFAULT: is_default}
    if text is not None:
        obj[LABEL_NAME] = text
    else:
        obj[LABEL_NAME] = str(value)
    return obj


def parameter_declaration(
        identifier, name=None, data_type=DT_STRING, description=None, index=0,
        required=True, values=None, parent=None, default_value=None,
        module=None, as_const=None
):
    """Create a dictionary that contains a module parameter specification.

    Raises InvalidParameterError if an invalid data type is given or if the
    identifier is None.

    Parameters
    ----------
    identifier: string
        Unique parameter identifier
    name: string, optional
        Printable parameter name. The default value is the parameter
        identifier.
    data_type: string, optional
        Parameter type. The default value is DT_STRING
    description: string, optional
        Optional text providing a more comprehensive description for the
        parameter. Default is the parameter name.
    index: int, optional
        Index position of argument in input form. The default value is 0.
    required: bool, optional
        Flag indicating whether values for this parameter are required or not
    values: list, optional
        List of valid parameter values (for selection in a front-end form)
    parent: string, optional
        Identifier of a grouping element
    module: string, optional
        Identifier of the module the parameter belongs to
    default_value: bool, string, number, optional
        Optional default value for a scalar parameter. Default values for file
        parameters are ignored.
    as_const: string, None
        Constant replacement value. This is primarily used to replace the name
        of uploaded files with a constant value.

    Returns
    -------
    dict

    Raises
    ------
    flowserv.core.error.InvalidParameterError
    """
    if identifier is None:
        raise InvalidParameterError('missing identifier')
    if data_type not in DATA_TYPES:
        msg = "invalid parameter data type '{}'"
        raise InvalidParameterError(msg.format(data_type))
    para = {
        LABEL_ID: identifier,
        LABEL_NAME: name if name is not None else identifier,
        LABEL_DATATYPE: data_type,
        LABEL_INDEX: index,
        LABEL_REQUIRED: required
    }
    # Set optional properties
    if description is not None:
        para[LABEL_DESCRIPTION] = description
    else:
        para[LABEL_DESCRIPTION] = para[LABEL_NAME]
    if values is not None:
        para[LABEL_VALUES] = values
    if parent is not None:
        para[LABEL_PARENT] = parent
    if as_const is not None:
        para[LABEL_AS] = as_const
    if default_value is not None:
        para[LABEL_DEFAULT] = default_value
    if module is not None:
        para[LABEL_MODULE] = module
    return para


def set_defaults(obj):
    """Set default values for parameter declaration if the respective element
    is missing in the given object. It is assumed that the parameter identifier
    is set in the given object.

    Returns a (modified) copy of the given parameter declaration object.

    Parameters
    ----------
    obj: dict
        Dictionary containing a parameter declaration

    Returns
    -------
    dict
    """
    para = dict(obj)
    # Set name to identifier value if no name is present
    set_value(para, LABEL_NAME, obj[LABEL_ID])
    # Set data type to DT_STRING if no data type is present
    set_value(para, LABEL_DATATYPE, DT_STRING)
    # Set description to name value if no description is present
    set_value(para, LABEL_DESCRIPTION, para[LABEL_NAME])
    # Set index to 0 if no parameter index is present
    set_value(para, LABEL_INDEX, 0)
    # Set required flag to True if not present
    set_value(para, LABEL_REQUIRED, True)
    return para


def set_value(parameter, key, value):
    """Set the value for the parameter element with given key to the given
    value if the element is currently not present in the parameter dictionary.
    If the element with key is present no changes occur.

    Parameters
    ----------
    parameter: dict
        Dictionary containing a parameter declaration
    key: string
        Parameter element label
    value: any
        Default element value
    """
    if key not in parameter:
        parameter[key] = value


def validate_parameter(param_declaration):
    """Validate a given parameter declaration.

    Raises a InvalidParameterError if an invalid parameter declaration is given
    or if the data type for the parameter is invalid.

    Parameters
    ----------
    param_declaration: dict
        Dictionary containing parameter declaration

    Raises
    ------
    flowserv.core.error.InvalidParameterError
    """
    # Make sure that the given package declaration matches the schema
    try:
        validate(param_declaration, PARAMETER_SCHEMA)
    except ValidationError as ex:
        msg = 'failed to validate parameter declaration.\n{}'
        raise InvalidParameterError(msg.format(ex.message))
    # Ensure that the given parameter data type is valid
    dt = param_declaration[LABEL_DATATYPE]
    if dt not in DATA_TYPES:
        raise InvalidParameterError("invalid data type '{}'".format(dt))
