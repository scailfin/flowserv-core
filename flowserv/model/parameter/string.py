# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Declarations for string parameter values. String parameters do not add any
additional properties to the base parameter class.
"""

from __future__ import annotations
from typing import Any, Dict, Optional

from flowserv.model.parameter.base import Parameter, PARA_STRING

import flowserv.error as err
import flowserv.model.parameter.base as pd
import flowserv.util as util


class String(Parameter):
    """String parameter type."""
    def __init__(
        self, name: str, index: Optional[int] = 0, label: Optional[str] = None,
        help: Optional[str] = None, default: Optional[str] = None,
        required: Optional[bool] = False, group: Optional[str] = None
    ):
        """Initialize the base properties a string parameter declaration.

        Parameters
        ----------
        name: string
            Unique parameter identifier
        index: int, default=0
            Index position of the parameter (for display purposes).
        label: string
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
        super(String, self).__init__(
            dtype=PARA_STRING,
            name=name,
            index=index,
            label=label,
            help=help,
            default=default,
            required=required,
            group=group
        )

    def cast(self, value: Any) -> Any:
        """Convert the given value into a string value.

        Parameters
        ----------
        value: any
            User-provided value for a template parameter.

        Returns
        -------
        sting
        """
        return str(value)

    @staticmethod
    def from_dict(doc: Dict, validate: Optional[bool] = True) -> String:
        """Get string parameter instance from a given dictionary serialization.

        Parameters
        ----------
        doc: dict
            Dictionary serialization for string parameter delaration.
        validate: bool, default=True
            Validate the serialized object if True.

        Returns
        -------
        flowserv.model.parameter.string.String

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
            if doc[pd.TYPE] != PARA_STRING:
                raise ValueError("invalid type '{}'".format(doc[pd.TYPE]))
        return String(
            name=doc[pd.NAME],
            index=doc[pd.INDEX],
            label=doc.get(pd.LABEL),
            help=doc.get(pd.HELP),
            default=doc.get(pd.DEFAULT),
            required=doc[pd.REQUIRED],
            group=doc.get(pd.GROUP)
        )
