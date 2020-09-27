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

from flowserv.model.parameter.base import ParameterBase

import flowserv.error as err
import flowserv.model.parameter.base as pd
import flowserv.util as util


"""Unique parameter type identifier."""
PARA_BOOL = 'bool'


class BoolParameter(ParameterBase):
    """Boolean parameter type."""
    def __init__(
        self, para_id, name, index, description=None,
        default_value=None, is_required=False, module_id=None
    ):
        """Initialize the base properties a Boolean parameter declaration.

        Parameters
        ----------
        para_id: string
            Unique parameter identifier
        name: string
            Human-readable parameter name.
        index: int
            Index position of the parameter (for display purposes).
        description: string, default=None
            Descriptive text for the parameter.
        default_value: any, default=None
            Optional default value.
        is_required: bool, default=False
            Is required flag.
        module_id: string, default=None
            Optional identifier for parameter group that this parameter
            belongs to.
        """
        super(BoolParameter, self).__init__(
            para_id=para_id,
            type_id=PARA_BOOL,
            name=name,
            index=index,
            description=description,
            default_value=default_value,
            is_required=is_required,
            module_id=module_id
        )

    @classmethod
    def from_dict(cls, doc, validate=True):
        """Get Boolean parameter instance from dictionary serialization.

        Parameters
        ----------
        doc: dict
            Dictionary serialization for a Boolean parameter.
        validate: bool, default=True
            Validate the serialized object if True.

        Returns
        -------
        flowserv.model.parameter.boolean.BoolParameter

        Raises
        ------
        flowserv.error.InvalidParameterError
        """
        if validate:
            try:
                util.validate_doc(
                    doc,
                    mandatory=[pd.ID, pd.TYPE, pd.NAME, pd.INDEX, pd.REQUIRED],
                    optional=[pd.DESC, pd.DEFAULT, pd.MODULE]
                )
            except ValueError as ex:
                raise err.InvalidParameterError(str(ex))
            if doc[pd.TYPE] != PARA_BOOL:
                raise ValueError("invalid type '{}'".format(doc[pd.TYPE]))
        return cls(
            para_id=doc[pd.ID],
            name=doc[pd.NAME],
            index=doc[pd.INDEX],
            description=doc.get(pd.DESC),
            default_value=doc.get(pd.DEFAULT),
            is_required=doc[pd.REQUIRED],
            module_id=doc.get(pd.MODULE)
        )

    def to_argument(self, value):
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

def is_bool(para: ParameterBase) -> bool:
    """Test if the given parameter is of type PARA_BOOL.

    Parameters
    ----------
    para: flowserv.model.parameter.base.ParameterBase
        Template parameter definition.

    Returns
    -------
    bool
    """
    return para.type_id == PARA_BOOL
