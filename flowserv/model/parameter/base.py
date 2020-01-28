# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Base class for workflow template parameters. Each parameter has a set of
properties that are used to (i) identify the parameter, (ii) define a nested
parameter structure, and (iii) render UI forms to collect parameter values.
"""

from flowserv.core.error import InvalidParameterError

import flowserv.core.error as err
import flowserv.core.util as util
import flowserv.model.parameter.declaration as pd


"""Special value for as-property that indicates user input for target path of
uploaded files.
"""
AS_INPUT = '$input'


"""Labels for serialized parameter group handles."""
LABEL_ID = 'id'
LABEL_INDEX = 'index'
LABEL_NAME = 'name'


class ParameterBase(object):
    """Base class for template parameters and parameter argument values. The
    base class maintains the unique parameter identifier and the information
    about the data type.
    """
    def __init__(self, identifier, data_type):
        """Initialize the unique identifier and data type. Raises an error if
        the given data type identifier is unknown.

        Parameters
        ----------
        identifier: string
            Unique parameter identifier
        data_type: string
            Identifier for parameter data type

        Raises
        ------
        flowserv.core.error.InvalidParameterError
        """
        if data_type not in pd.DATA_TYPES:
            msg = "invalid data type '{}'"
            raise InvalidParameterError(msg.format(data_type))
        self.identifier = identifier
        self.data_type = data_type

    def is_bool(self):
        """Test if data type for the parameter declaration is DT_BOOL.

        Returns
        -------
        bool
        """
        return self.data_type == pd.DT_BOOL

    def is_file(self):
        """Test if data type for the parameter declaration is DT_FILE.

        Returns
        -------
        bool
        """
        return self.data_type == pd.DT_FILE

    def is_float(self):
        """Test if data type for the parameter declaration is DT_DECIMAL.

        Returns
        -------
        bool
        """
        return self.data_type == pd.DT_DECIMAL

    def is_int(self):
        """Test if data type for the parameter declaration is DT_INTEGER.

        Returns
        -------
        bool
        """
        return self.data_type == pd.DT_INTEGER

    def is_list(self):
        """Test if data type for the parameter declaration is DT_LIST.

        Returns
        -------
        bool
        """
        return self.data_type == pd.DT_LIST

    def is_record(self):
        """Test if data type for the parameter declaration is DT_RECORD.

        Returns
        -------
        bool
        """
        return self.data_type == pd.DT_RECORD

    def is_string(self):
        """Test if data type for the parameter declaration is DT_STRING.

        Returns
        -------
        bool
        """
        return self.data_type == pd.DT_STRING


class ParameterGroup(object):
    """Parameter groups are identifiable sets of parameters. These sets are
    primarily intended for display purposes in the front-end. Therefore, each
    group has a display name and an index position that defines the sort order
    for groups.
    """
    def __init__(self, identifier, name, index):
        """Initialize the object properties.

        Parameters
        ----------
        identifier: string
            Unique group identifier
        name: string
            Human-readable group name
        index: int
            Group sort order index
        """
        self.identifier = identifier
        self.name = name
        self.index = index

    @classmethod
    def from_dict(cls, doc):
        """Create object instance from dictionary serialization.

        Parameters
        ----------
        doc: dict
            Dictionary serialization for parameter group handles

        Returns
        -------
        flowserv.module.parameter.base.ParameterGroup

        Raises
        ------
        ValueError
        """
        util.validate_doc(
            doc,
            mandatory=[LABEL_ID, LABEL_NAME, LABEL_INDEX]
        )
        return cls(
            identifier=doc[LABEL_ID],
            name=doc[LABEL_NAME],
            index=doc[LABEL_INDEX]
        )

    def to_dict(self):
        """Get dictionary serialization for parameter group handle.

        Returns
        -------
        dict
        """
        return {
            LABEL_ID: self.identifier,
            LABEL_NAME: self.name,
            LABEL_INDEX: self.index
        }


class TemplateParameter(ParameterBase):
    """The template parameter is a simple wrapper around a dictionary that
    contains a parameter declaration. The wrapper provides easy access to the
    different components of the parameter declaration.
    """
    def __init__(self, obj, children=None):
        """Initialize the different attributes of a template parameter
        declaration from a given dictionary.

        Parameters
        ----------
        obj: dict
            Dictionary containing the template parameter declaration properties
        children: list(flowserv.model.parameter.base.TemplateParameter), optional
            Optional list of parameter children for parameter lists or records
        """
        super(TemplateParameter, self).__init__(
            identifier=obj[pd.LABEL_ID],
            data_type=obj[pd.LABEL_DATATYPE]
        )
        self.obj = obj
        self.name = obj.get(pd.LABEL_NAME, self.identifier)
        self.description = obj.get(pd.LABEL_DESCRIPTION)
        self.index = obj.get(pd.LABEL_INDEX, 0)
        self.default_value = obj.get(pd.LABEL_DEFAULT)
        self.is_required = obj.get(pd.LABEL_REQUIRED, False)
        self.values = obj.get(pd.LABEL_VALUES)
        self.parent = obj.get(pd.LABEL_PARENT)
        self.as_constant = obj.get(pd.LABEL_AS)
        self.module = obj.get(pd.LABEL_MODULE)
        self.children = children

    def add_child(self, para):
        """Short-cut to add an element to the list of children of the
        parameter.

        Parameters
        ----------
        para: flowserv.model.parameter.base.TemplateParameter
            Template parameter instance for child parameter
        """
        self.children.append(para)
        self.children.sort(key=lambda p: (p.index, p.identifier))

    def as_input(self):
        """Flag indicating whether the value for the as constant property is
        the special value that indicates that the property value is provided
        by the user.
        """
        return self.as_constant == AS_INPUT

    def get_constant(self):
        """Get the value of the as_constant property.

        Returns
        -------
        string
        """
        return self.as_constant

    def has_children(self):
        """Test if a parameter has children. Only returns True if the list of
        children is not None and not empty.

        Returns
        -------
        bool
        """
        if self.children is not None:
            return len(self.children) > 0
        return False

    def has_constant(self):
        """True if the as_constant property is not None.

        Returns
        -------
        bool
        """
        return self.as_constant is not None

    def merge(self, para):
        """Merge the parameter with the values of a given parameter. This will
        only affect the following properties: name, description, index,
        is_required, default value, values, and module.

        Returns a modified copy of the parameter.

        Parameters
        ----------
        para: flowserv.model.parameter.base.TemplateParameter
            Declaration of the modified parameter

        Returns
        -------
        flowserv.model.parameter.base.TemplateParameter
        """
        obj = dict(self.obj)
        obj[pd.LABEL_NAME] = para.obj[pd.LABEL_NAME]
        obj[pd.LABEL_DESCRIPTION] = para.obj[pd.LABEL_DESCRIPTION]
        obj[pd.LABEL_INDEX] = para.obj[pd.LABEL_INDEX]
        obj[pd.LABEL_REQUIRED] = para.obj[pd.LABEL_REQUIRED]
        if pd.LABEL_DEFAULT in para.obj:
            obj[pd.LABEL_DEFAULT] = para.obj[pd.LABEL_DEFAULT]
        if pd.LABEL_VALUES in para.obj:
            obj[pd.LABEL_VALUES] = para.obj[pd.LABEL_VALUES]
        if pd.LABEL_MODULE in para.obj:
            obj[pd.LABEL_MODULE] = para.obj[pd.LABEL_MODULE]
        return TemplateParameter(obj=obj, children=self.children)

    def prompt(self):
        """Get default input prompt for the parameter declaration. The prompt
        contains an indication of the data type, the parameter name and the
        default value (if defined).

        Returns
        -------
        string
        """
        val = str(self.name)
        # Add text that indicates the parameter type
        if self.is_bool():
            val += ' (bool)'
        elif self.is_file():
            val += ' (file)'
        elif self.is_float():
            val += ' (decimal)'
        elif self.is_int():
            val += ' (integer)'
        else:
            val += ' (string)'
        if self.default_value is not None:
            if self.is_bool() or self.is_float() or self.is_int():
                val += ' [default ' + str(self.default_value) + ']'
            else:
                val += ' [default \'' + str(self.default_value) + '\']'
        return val + ': '

    def to_dict(self):
        """Get the dictionary serialization for the parameter declaration.

        Returns
        -------
        dict
        """
        return self.obj


# -- Helper Functions ---------------------------------------------------------

def create_parameter_index(parameters, validate=True):
    """Create instances of template parameters from a list of dictionaries
    containing parameter declarations. The result is a dictionary containing
    the top-level parameters, indexed by their unique identifier.

    Parameters
    ----------
    parameters: list(dict)
        List of dictionaries containing template parameter declarations
    validate: bool, optional
        Flag indicating if given template parameter declarations are to be
        validated against the parameter schema or not.

    Returns
    -------
    dict(string: flowserv.model.parameter.base.TemplateParameter)

    Raises
    ------
    flowserv.core.error.InvalidTemplateError
    flowserv.core.error.UnknownParameterError
    """
    result = dict()
    for para in parameters:
        # Validate the template parameters if the validate flag is True
        if validate:
            pd.validate_parameter(para)
        # Create a TemplateParameter instance for the parameter. Keep
        # track of children for parameter that are of type DT_LIST or
        # DT_RECORD. Children are added after all parameters have been
        # instantiated.
        p_id = para[pd.LABEL_ID]
        # Ensure that the identifier of all parameters are unique
        if p_id in result:
            msg = "parameter '{}' not unique"
            raise err.InvalidTemplateError(msg.format(p_id))
        c = None
        if para[pd.LABEL_DATATYPE] in [pd.DT_LIST, pd.DT_RECORD]:
            c = list()
        tp = TemplateParameter(pd.set_defaults(para), children=c)
        result[p_id] = tp
    # Add parameter templates to the list of children for their
    # respective parent (if given). We currently only support one level
    # of nesting.
    for para in parameters:
        if pd.LABEL_PARENT in para:
            p_id = para[pd.LABEL_ID]
            parent = para[pd.LABEL_PARENT]
            if parent is not None:
                result[parent].add_child(result[p_id])
    return result
