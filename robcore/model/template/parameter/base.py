# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Base class for workflow template parameters. Each parameter has a set of
properties that are used to (i) identify the parameter, (ii) define a nested
parameter structure, and (iii) render UI forms to collect parameter values.
"""

from robcore.error import InvalidParameterError

import robcore.model.template.parameter.declaration as pd


"""Special value for as-property that indicates user input for target path of
uploaded files.
"""
AS_INPUT = '$input'


class ParameterBase(object):
    """Base class for template parameter and parameter argument values. The
    base class maintains the unique parameter identifier and the information
    about the data type.
    """
    def __init__(self, identifier, data_type):
        """Initialize the unique identifier and data type. Raises value error
        if the given data type identifier is not valid.

        Parameters
        ----------
        identifier: string
            Unique parameter identifier
        data_type: string
            Identifier for parameter data type

        Raises
        ------
        robcore.error.InvalidParameterError
        """
        if not data_type in pd.DATA_TYPES:
            raise InvalidParameterError('invalid data type \'{}\''.format(data_type))
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
        children: list(robcore.model.template.parameter.base.TemplateParameter), optional
            Optional list of parameter children for parameter lists or records
        """
        super(TemplateParameter, self).__init__(
            identifier=obj[pd.LABEL_ID],
            data_type = obj[pd.LABEL_DATATYPE]
        )
        self.obj = obj
        self.name = obj[pd.LABEL_NAME]
        self.description = obj[pd.LABEL_DESCRIPTION]
        self.index = obj[pd.LABEL_INDEX]
        self.default_value = obj[pd.LABEL_DEFAULT] if pd.LABEL_DEFAULT in obj else None
        self.is_required = obj[pd.LABEL_REQUIRED]
        self.values = obj[pd.LABEL_VALUES] if pd.LABEL_VALUES in obj else None
        self.parent = obj[pd.LABEL_PARENT] if pd.LABEL_PARENT in obj else None
        self.as_constant = obj[pd.LABEL_AS] if pd.LABEL_AS in obj else None
        self.children = children

    def add_child(self, para):
        """Short-cut to add an element to the list of children of the parameter.

        Parameters
        ----------
        para: robcore.model.template.parameter.base.TemplateParameter
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
        if not self.children is None:
            return len(self.children) > 0
        return False

    def has_constant(self):
        """True if the as_constant property is not None.

        Returns
        -------
        bool
        """
        return not self.as_constant is None

    def merge(self, para):
        """Merge the parameter with the values of a given parameter. This will
        only affect the following parameter properties: name, description

        Returns a modified copy of the parameter.

        Parameters
        ----------
        para: robcore.model.template.parameter.base.TemplateParameter
            Declaration of the modified parameter

        Returns
        -------
        robcore.model.template.parameter.base.TemplateParameter
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
        if not self.default_value is None:
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
