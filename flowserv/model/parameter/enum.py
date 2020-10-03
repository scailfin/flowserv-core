# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Declarations for enumeration parameter values. Enumeration parameters
contain a list of valid parameter values. These values are defined by a
printable 'name' and an associated 'value'.
"""

from flowserv.model.parameter.base import ParameterBase

import flowserv.error as err
import flowserv.model.parameter.base as pd
import flowserv.util as util


"""Unique parameter type identifier."""
PARA_ENUM = 'enum'


class EnumParameter(ParameterBase):
    """Enumeration parameter type. Extends the base parameter with a list of
    possible argument values.
    """
    def __init__(
        self, para_id, name, index, values, description=None,
        default_value=None, is_required=False, module_id=None
    ):
        """Initialize the base properties a enumeration parameter declaration.

        Parameters
        ----------
        para_id: string
            Unique parameter identifier
        name: string
            Human-readable parameter name.
        index: int
            Index position of the parameter (for display purposes).
        values: list
            List of dictionary serializations containing enumeration of valid
            parameter values.
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
        super(EnumParameter, self).__init__(
            para_id=para_id,
            type_id=PARA_ENUM,
            name=name,
            index=index,
            description=description,
            default_value=default_value,
            is_required=is_required,
            module_id=module_id
        )
        self.values = values

    @classmethod
    def from_dict(cls, doc, validate=True):
        """Get enumeration parameter instance from dictionary serialization.

        Parameters
        ----------
        doc: dict
            Dictionary serialization for enumeration parameter.
        validate: bool, default=True
            Validate the serialized object if True.

        Returns
        -------
        flowserv.model.parameter.enum.EnumParameter

        Raises
        ------
        flowserv.error.InvalidParameterError
        """
        if validate:
            try:
                util.validate_doc(
                    doc,
                    mandatory=[
                        pd.ID,
                        pd.TYPE,
                        pd.NAME,
                        pd.INDEX,
                        pd.REQUIRED,
                        'values'
                    ],
                    optional=[pd.DESC, pd.DEFAULT, pd.MODULE]
                )
                for val in doc['values']:
                    util.validate_doc(
                        val,
                        mandatory=['name', 'value'],
                        optional=['isDefault']
                    )
            except ValueError as ex:
                raise err.InvalidParameterError(str(ex))
            if doc[pd.TYPE] != PARA_ENUM:
                raise ValueError("invalid type '{}'".format(doc[pd.TYPE]))
        return cls(
            para_id=doc[pd.ID],
            name=doc[pd.NAME],
            index=doc[pd.INDEX],
            description=doc.get(pd.DESC),
            default_value=doc.get(pd.DEFAULT),
            is_required=doc[pd.REQUIRED],
            module_id=doc.get(pd.MODULE),
            values=doc['values']
        )

    def to_argument(self, value):
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

    def to_dict(self):
        """Get dictionary serialization for the parameter declaration. Adds
        list of enumerated values to the base serialization.

        Returns
        -------
        dict
        """
        obj = super().to_dict()
        obj['values'] = self.values
        return obj


# -- Helper Methods -----------------------------------------------------------

def is_enum(para: ParameterBase) -> bool:
    """Test if the given parameter is of type PARA_ENUM.

    Parameters
    ----------
    para: flowserv.model.parameter.base.ParameterBase
        Template parameter definition.

    Returns
    -------
    bool
    """
    return para.type_id == PARA_ENUM
