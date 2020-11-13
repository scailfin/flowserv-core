# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Declarations for Boolean parameter values. Boolean parameters do not add any
additional properties to the base parameter class.
"""

from typing import Any, Dict, Optional
from flowserv.model.parameter.base import Parameter

import flowserv.error as err
import flowserv.model.parameter.base as pd
import flowserv.util as util


"""Unique parameter type identifier."""
PARA_BOOL = 'bool'


class Bool(Parameter):
    """Boolean parameter type."""
    def __init__(
        self, name: str, index: int, label: Optional[str] = None,
        help: Optional[str] = None, default: Optional[bool] = None,
        required: Optional[bool] = False, module: Optional[str] = None
    ):
        """Initialize the base properties a Boolean parameter declaration.

        Parameters
        ----------
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
        super(Bool, self).__init__(
            dtype=PARA_BOOL,
            name=name,
            index=index,
            label=label,
            help=help,
            default=default,
            required=required,
            module=module
        )

    @classmethod
    def from_dict(cls, doc: Dict, validate: Optional[bool] = True):
        """Get Boolean parameter instance from dictionary serialization.

        Parameters
        ----------
        doc: dict
            Dictionary serialization for a Boolean parameter.
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
            try:
                util.validate_doc(doc, mandatory=pd.MANDATORY, optional=pd.OPTIONAL)
            except ValueError as ex:
                raise err.InvalidParameterError(str(ex))
            if doc[pd.TYPE] != PARA_BOOL:
                raise ValueError("invalid type '{}'".format(doc[pd.TYPE]))
        return cls(
            name=doc[pd.NAME],
            index=doc[pd.INDEX],
            label=doc[pd.LABEL],
            help=doc.get(pd.HELP),
            default=doc.get(pd.DEFAULT),
            required=doc[pd.REQUIRED],
            module=doc.get(pd.MODULE)
        )

    def to_argument(self, value: Any) -> Any:
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


# -- Helper Methods -----------------------------------------------------------

def is_bool(para: Parameter) -> bool:
    """Test if the given parameter is of type PARA_BOOL.

    Parameters
    ----------
    para: flowserv.model.parameter.base.Parameter
        Template parameter definition.

    Returns
    -------
    bool
    """
    return para.dtype == PARA_BOOL
