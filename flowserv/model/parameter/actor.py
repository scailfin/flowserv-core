# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Declarations for parameters that represent actors (steps) in a workflow. The
actual object instances that result from these actors are implementation
dependent. For this reason, the values for actor parameters are represented as
serialized dictionaries.
"""

from __future__ import annotations
from typing import Dict, Optional

from flowserv.model.parameter.base import Parameter, PARA_ACTOR

import flowserv.error as err
import flowserv.model.parameter.base as pd
import flowserv.util as util


class Actor(Parameter):
    """Workflow actor parameter type."""
    def __init__(
        self, name: str, index: Optional[int] = 0, label: Optional[str] = None,
        help: Optional[str] = None, default: Optional[str] = None,
        required: Optional[bool] = False, group: Optional[str] = None
    ):
        """Initialize the base properties a actor parameter declaration.

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
        super(Actor, self).__init__(
            dtype=PARA_ACTOR,
            name=name,
            index=index,
            label=label,
            help=help,
            default=default,
            required=required,
            group=group
        )

    def cast(self, value: Dict) -> Dict:
        """Ensure that the given value is a dictionary (serialization of a
        workflow step).

        Serializations of workflow steps are implementation dependent. For this
        reason, the function does not further validate the contents of the
        dictionary.

        Raises an InvalidArgumentError if the argument value is not a
        dictionary.

        Parameters
        ----------
        value: dict
            Dictionary serialization for a workflow actor.

        Returns
        -------
        dict
        """
        if not isinstance(value, dict):
            raise err.InvalidArgumentError("invalid actor value '{}'".format(value))
        return value

    @staticmethod
    def from_dict(doc: Dict, validate: Optional[bool] = True) -> Actor:
        """Get an actor parameter instance from a given dictionary
        serialization.

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
            if doc[pd.TYPE] != PARA_ACTOR:
                raise ValueError("invalid type '{}'".format(doc[pd.TYPE]))
        return Actor(
            name=doc[pd.NAME],
            index=doc[pd.INDEX],
            label=doc.get(pd.LABEL),
            help=doc.get(pd.HELP),
            default=doc.get(pd.DEFAULT),
            required=doc[pd.REQUIRED],
            group=doc.get(pd.GROUP)
        )
