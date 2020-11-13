# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Base class for workflow template parameters. Each parameter has a set of
properties that are used to (i) identify the parameter, (ii) define a nested
parameter structure, and (iii) render UI forms to collect parameter values.
"""

from abc import ABCMeta, abstractmethod
from typing import Any, Dict, Optional

import flowserv.util as util


"""Labels for general workflow declaration elements."""

DEFAULT = 'defaultValue'
HELP = 'help'
INDEX = 'index'
LABEL = 'label'
MODULE = 'module'
NAME = 'name'
TYPE = 'dtype'
REQUIRED = 'isRequired'

MANDATORY = [NAME, TYPE, INDEX, REQUIRED]
OPTIONAL = [LABEL, HELP, DEFAULT, MODULE]


class Parameter(metaclass=ABCMeta):
    """Base class for template parameters. The base class maintains the unique
    parameter name, the data type identifier, the human-readable label and the
    description for display purposes, the is required flag, an optional default
    value, the index position for input form rendering, and the identifier for
    the parameter group.

    Implementing classes have to provide a static .from_dict() method that
    returns an instance of the class from a dictionary serialization. The
    dictionary serializations for each class are generated by the .to_dict()
    method.
    """
    def __init__(
        self, dtype: str, name: str, index: int, label: Optional[str] = None,
        help: Optional[str] = None, default: Optional[Any] = None,
        required: Optional[bool] = False, module: Optional[str] = None
    ):
        """Initialize the base properties for a template parameter.

        Parameters
        ----------
        dtype: string
            Parameter type identifier.
        name: string
            Unique parameter identifier
        index: int
            Index position of the parameter (for display purposes).
        label: string, default=None
            Human-readable parameter name.
        help: string, default=None
            Descriptive text for the parameter.
        default: any, default=None
            Optional default value.
        required: bool, default=False
            Is required flag.
        module: string, default=None
            Optional identifier for parameter group that this parameter
            belongs to.
        """
        self.dtype = dtype
        self.name = name
        self.index = index
        self.label = label
        self.help = help
        self.default = default
        self.required = required
        self.module = module

    def display_name(self) -> str:
        """Human-readable display name for the parameter. The default display
        name is the defined label. If no label is defined the parameter name is
        returned.

        Returns
        -------
        str
        """
        return self.label if self.label is not None else self.name

    def prompt(self) -> str:
        """Get default input prompt for the parameter declaration. The prompt
        contains an indication of the data type, the parameter name and the
        default value (if defined).

        Returns
        -------
        string
        """
        val = '{} ({})'.format(self.display_name(), self.dtype)
        if self.default is not None:
            val += " [default '{}']".format(self.default)
        return val + ' $> '

    @abstractmethod
    def to_argument(self, value: Any) -> Any:
        """Validate the given argument value for the parameter type. Returns
        the argument representation for the value that is used to replace
        references to the parameter in workflow templates.

        Raises an InvalidArgumentError if the given value is not valid for the
        parameter type.

        Parameters
        ----------
        value: any
            User-provided value for a template parameter.

        Returns
        -------
        sting, float, or int

        Raises
        ------
        flowserv.error.InvalidArgumentError
        """
        raise NotImplementedError()  # pragma: no cover

    def to_dict(self) -> Dict:
        """Get dictionary serialization for the parameter declaration.
        Implementing classes can add elements to the base dictionary.

        Returns
        -------
        dict
        """
        return {
            TYPE: self.dtype,
            NAME: self.name,
            INDEX: self.index,
            LABEL: self.label,
            HELP: self.help,
            DEFAULT: self.default,
            REQUIRED: self.required,
            MODULE: self.module
        }


class ParameterGroup(object):
    """Parameter groups are identifiable sets of parameters. These sets are
    primarily intended for display purposes in the front-end. Therefore, each
    group has a display name and an index position that defines the sort order
    for groups.
    """
    def __init__(self, identifier: str, name: str, index: int):
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
    def from_dict(cls, doc, validate=True):
        """Create object instance from dictionary serialization.

        Parameters
        ----------
        doc: dict
            Dictionary serialization for parameter group handles
        validate: bool, default=True
            Validate the serialization if True.

        Returns
        -------
        flowserv.model.parameter.base.ParameterGroup

        Raises
        ------
        ValueError
        """
        if validate:
            util.validate_doc(
                doc,
                mandatory=['id', 'name', 'index']
            )
        return cls(
            identifier=doc['id'],
            name=doc['name'],
            index=doc['index']
        )

    def to_dict(self):
        """Get dictionary serialization for parameter group handle.

        Returns
        -------
        dict
        """
        return {
            'id': self.identifier,
            'name': self.name,
            'index': self.index
        }
