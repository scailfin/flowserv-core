# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Declarations for Boolean parameter values. Boolean parameters do not add any
additional properties to the base parameter class.
"""

from __future__ import annotations
from typing import Any, Dict, Optional
from flowserv.model.parameter.base import Parameter, PARA_BOOL

import flowserv.error as err
import flowserv.model.parameter.base as pd
import flowserv.util as util


class Bool(Parameter):
    """Boolean parameter type."""
    def __init__(
        self, name: str, index: Optional[int] = 0, label: Optional[str] = None,
        help: Optional[str] = None, default: Optional[bool] = None,
        required: Optional[bool] = False, group: Optional[str] = None
    ):
        """Initialize the base properties a Boolean parameter declaration.

        Parameters
        ----------
        name: string
            Unique parameter identifier
        index: int, default=0
            Index position of the parameter (for display purposes).
        label: string, default=None
            Human-readable parameter name.
        help: string, default=None
            Descriptive text for the parameter.
        default: any, default=None
            Optional default value.
        required: bool, default=False
            Is required flag.
        group: string, default=None
            Optional identifier for parameter group that this parameter
            belongs to.
        """
        super(Bool, self).__init__(
            dtype=PARA_BOOL,
            name=name,
            index=index,
            label=label,
            help=help,
            default=default,
            required=required,
            group=group
        )

    def cast(self, value: Any) -> Any:
        """Convert the given value into a Boolean value. Converts string values
        to Boolean True if they match either of the string representations '1',
        't' or 'true' (case-insensitive) and to False if the value is None or
        it matches '', '0', 'f' or 'false'. Raises an error if a given value is
        not a valid representation for a Boolean value.

        Parameters
        ----------
        value: any
            User-provided value for a template parameter.

        Returns
        -------
        sting

        Raises
        ------
        flowserv.error.InvalidArgumentError
        """
        if value is None:
            return False
        if isinstance(value, bool):
            return value
        strvalue = str(value).lower()
        if strvalue in ['1', 't', 'true']:
            return True
        elif strvalue in ['', '0', 'f', 'false']:
            return False
        raise err.InvalidArgumentError("not a Boolean '{}'".format(value))

    @staticmethod
    def from_dict(doc: Dict, validate: Optional[bool] = True) -> Bool:
        """Get Boolean parameter instance from a dictionary serialization.

        Parameters
        ----------
        doc: dict
            Dictionary serialization for a Boolean parameter declaration.
        validate: bool, default=True
            Validate the serialized object if True.

        Returns
        -------
        flowserv.model.parameter.boolean.Bool

        Raises
        ------
        flowserv.error.InvalidParameterError
        """
        if validate:
            util.validate_doc(
                doc,
                mandatory=pd.MANDATORY,
                optional=pd.OPTIONAL,
                exception=err.InvalidParameterError
            )
            if doc[pd.TYPE] != PARA_BOOL:
                raise ValueError("invalid type '{}'".format(doc[pd.TYPE]))
        return Bool(
            name=doc[pd.NAME],
            index=doc[pd.INDEX],
            label=doc.get(pd.LABEL),
            help=doc.get(pd.HELP),
            default=doc.get(pd.DEFAULT),
            required=doc[pd.REQUIRED],
            group=doc.get(pd.GROUP)
        )
