# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Declarations for list parameters. List parameter values are lists of values
for a single parameter declaration.
"""

from __future__ import annotations
from typing import Dict, List, Optional

from flowserv.model.parameter.base import Parameter, PARA_LIST

import flowserv.error as err
import flowserv.model.parameter.base as pd
import flowserv.util as util


class Array(Parameter):
    """List parameter type to define lists of values that all match the same
    parameter declaration..
    """
    def __init__(
        self, name: str, para: Parameter, index: Optional[int] = 0,
        label: Optional[str] = None, help: Optional[str] = None,
        default: Optional[List] = None, required: Optional[bool] = False,
        group: Optional[str] = None
    ):
        """Initialize the base properties for a list parameter declaration.

        Parameters
        ----------
        name: string
            Unique parameter identifier
        para: flowserv.model.parameter.base.Parameter
            Declaration for the parameter that defines the list values.
        index: int, default=0
            Index position of the parameter (for display purposes).
        label: string
            Human-readable parameter name.
        help: string, default=None
            Descriptive text for the parameter.
        default: list, default=None
            Optional default value.
        required: bool, default=False
            Is required flag.
        group: string, default=None
            Optional identifier for parameter group that this parameter
            belongs to.
        """
        super(Array, self).__init__(
            dtype=PARA_LIST,
            name=name,
            index=index,
            label=label,
            help=help,
            default=default,
            required=required,
            group=group
        )
        self.para = para

    def cast(self, value: List) -> List:
        """Convert the given value into a list where each value in the given
        list has been converted using the parameter declaration for the list
        parameter.

        Raises an error if the value is not a list or if the associated parameter
        declaration raised an exception during cast.

        Parameters
        ----------
        value: any
            User-provided value for a template parameter.

        Returns
        -------
        list

        Raises
        ------
        flowserv.error.InvalidArgumentError
        """
        if not isinstance(value, list):
            raise err.InvalidArgumentError('invalid argument type')
        return [self.para.cast(v) for v in value]

    @staticmethod
    def from_dict(doc: Dict, validate: Optional[bool] = True) -> List:
        """Get list parameter instance from a dictionary serialization.

        Parameters
        ----------
        doc: dict
            Dictionary serialization for a list parameter declaration.
        validate: bool, default=True
            Validate the serialized object if True.

        Returns
        -------
        flowserv.model.parameter.list.List

        Raises
        ------
        flowserv.error.InvalidParameterError
        """
        if validate:
            util.validate_doc(
                doc,
                mandatory=pd.MANDATORY + ['para'],
                optional=pd.OPTIONAL,
                exception=err.InvalidParameterError
            )
            if doc[pd.TYPE] != PARA_LIST:
                raise ValueError("invalid type '{}'".format(doc[pd.TYPE]))
        # Deserialize parameter declaration. Import the deserializer here to
        # avoid cyclic dependencies.
        from flowserv.model.parameter.factory import ParameterDeserializer as deserializer
        return Array(
            name=doc[pd.NAME],
            para=deserializer.from_dict(doc['para'], validate=validate),
            index=doc[pd.INDEX],
            label=doc.get(pd.LABEL),
            help=doc.get(pd.HELP),
            default=doc.get(pd.DEFAULT),
            required=doc[pd.REQUIRED],
            group=doc.get(pd.GROUP)
        )

    def to_dict(self) -> Dict:
        """Get dictionary serialization for the parameter declaration. Adds
        the serialized value for the list parameter declaration.

        Returns
        -------
        dict
        """
        obj = super().to_dict()
        obj['para'] = self.para.to_dict()
        return obj
