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

from flowserv.model.parameter.base import ParameterBase

import flowserv.error as err
import flowserv.model.parameter.base as pd
import flowserv.util as util


"""Unique parameter type identifier."""
PARA_STRING = 'string'


class StringParameter(ParameterBase):
    """String parameter type."""
    def __init__(
        self, para_id, name, index, description=None,
        default_value=None, is_required=False, module_id=None
    ):
        """Initialize the base properties a string parameter declaration.

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
        super(StringParameter, self).__init__(
            para_id=para_id,
            type_id=PARA_STRING,
            name=name,
            index=index,
            description=description,
            default_value=default_value,
            is_required=is_required,
            module_id=module_id
        )

    @classmethod
    def from_dict(cls, doc, validate=True):
        """Get string parameter instance from dictionary serialization.

        Parameters
        ----------
        doc: dict
            Dictionary serialization for string parameter.
        validate: bool, default=True
            Validate the serialized object if True.

        Returns
        -------
        flowserv.model.parameter.string.StringParameter

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
            if doc[pd.TYPE] != PARA_STRING:
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
        """Convert the given value into a string value. Raises an error if the
        value is None and the is_required flag for the parameter is True.

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
        if value is None and self.is_required:
            raise err.InvalidArgumentError('missing argument')
        return str(value)
