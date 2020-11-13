# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Declarations for string parameter values. String parameters do not add any
additional properties to the base parameter class.
"""

from typing import Any, Dict, Optional

from flowserv.model.parameter.base import Parameter

import flowserv.error as err
import flowserv.model.parameter.base as pd
import flowserv.util as util


"""Unique parameter type identifier."""
PARA_STRING = 'string'


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

    @classmethod
    def from_dict(cls, doc: Dict, validate: Optional[bool] = True):
        """Get string parameter instance from dictionary serialization.

        Parameters
        ----------
        doc: dict
            Dictionary serialization for string parameter.
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
            try:
                util.validate_doc(doc, mandatory=pd.MANDATORY, optional=pd.OPTIONAL)
            except ValueError as ex:
                raise err.InvalidParameterError(str(ex))
            if doc[pd.TYPE] != PARA_STRING:
                raise ValueError("invalid type '{}'".format(doc[pd.TYPE]))
        return cls(
            name=doc[pd.NAME],
            index=doc[pd.INDEX],
            label=doc[pd.LABEL],
            help=doc.get(pd.HELP),
            default=doc.get(pd.DEFAULT),
            required=doc[pd.REQUIRED],
            group=doc.get(pd.GROUP)
        )

    def to_argument(self, value: Any) -> Any:
        """Convert the given value into a string value. Raises an error if the
        value is None and the is required flag for the parameter is True.

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
        if value is None and self.required:
            raise err.InvalidArgumentError('missing argument')
        return str(value)
