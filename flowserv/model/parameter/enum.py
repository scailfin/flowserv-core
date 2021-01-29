# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Declarations for enumeration parameter values. Enumeration parameters
contain a list of valid parameter values. These values are defined by a
printable 'name' and an associated 'value'.
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional, Union

from flowserv.model.parameter.base import Parameter, PARA_SELECT

import flowserv.error as err
import flowserv.model.parameter.base as pd
import flowserv.util as util


class Select(Parameter):
    """Enumeration parameter type for select boxes. Extends the base parameter
    with a list of possible argument values.
    """
    def __init__(
        self, name: str, values: List[Dict], index: Optional[int] = 0,
        label: Optional[str] = None, help: Optional[str] = None,
        default: Optional[bool] = None, required: Optional[bool] = False,
        group: Optional[str] = None
    ):
        """Initialize the base properties for a select parameter declaration.

        Parameters
        ----------
        name: string
            Unique parameter identifier
        index: int, default=0
            Index position of the parameter (for display purposes).
        values: list
            List of dictionary serializations containing enumeration of valid
            parameter values.
        label: string, default=None
            Human-readable parameter name.
        help: string, default=None
            Descriptive text for the parameter.
        default: bool, default=None
            Optional default value.
        required: bool, default=False
            Is required flag.
        group: string, default=None
            Optional identifier for parameter group that this parameter
            belongs to.
        """
        super(Select, self).__init__(
            dtype=PARA_SELECT,
            name=name,
            index=index,
            label=label,
            help=help,
            default=default,
            required=required,
            group=group
        )
        self.values = values

    def cast(self, value: Any) -> Any:
        """Ensure that the given value is valid. If the value is not contained
        in the enumerated list of values an error is raised.

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
        for val in self.values:
            if val['value'] == value:
                return value
        raise err.InvalidArgumentError("unknown value '{}'".format(value))

    @staticmethod
    def from_dict(doc: Dict, validate: Optional[bool] = True) -> Select:
        """Get select parameter instance from a dictionary serialization.

        Parameters
        ----------
        doc: dict
            Dictionary serialization for select parameter declaration.
        validate: bool, default=True
            Validate the serialized object if True.

        Returns
        -------
        flowserv.model.parameter.enum.Select

        Raises
        ------
        flowserv.error.InvalidParameterError
        """
        if validate:
            util.validate_doc(
                doc,
                mandatory=pd.MANDATORY + ['values'],
                optional=pd.OPTIONAL,
                exception=err.InvalidParameterError
            )
            for val in doc['values']:
                util.validate_doc(
                    val,
                    mandatory=['name', 'value'],
                    optional=['isDefault'],
                    exception=err.InvalidParameterError
                )
            if doc[pd.TYPE] != PARA_SELECT:
                raise ValueError("invalid type '{}'".format(doc[pd.TYPE]))
        return Select(
            name=doc[pd.NAME],
            index=doc[pd.INDEX],
            label=doc.get(pd.LABEL),
            help=doc.get(pd.HELP),
            default=doc.get(pd.DEFAULT),
            required=doc[pd.REQUIRED],
            group=doc.get(pd.GROUP),
            values=doc['values']
        )

    def to_dict(self) -> Dict:
        """Get dictionary serialization for the parameter declaration. Adds
        list of enumerated values to the base serialization.

        Returns
        -------
        dict
        """
        obj = super().to_dict()
        obj['values'] = self.values
        return obj


def Option(name: str, value: Union[str, int], default: Optional[bool] = None) -> Dict:
    """Get a dictionary serialization for an element in the enumeration of valid
    values for a select parameter.

    Parameters
    ----------
    name: string
        Option display name.
    value: string or int
        Returned value if this option is selected.
    default: bool, default=None
        Indicate if this is the default option for the selection.

    Returns
    -------
    dict
    """
    doc = {'name': name, 'value': value}
    if default is not None:
        doc['isDefault'] = default
    return doc
