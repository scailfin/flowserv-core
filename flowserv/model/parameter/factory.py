# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Factory for parameter declarations. Allows to create instances of parameter
declaration classes from dictionary serializations.
"""

from typing import Dict, Optional

from flowserv.model.parameter.base import Parameter, TYPE
from flowserv.model.parameter.base import (
    PARA_BOOL, PARA_FILE, PARA_FLOAT, PARA_INT, PARA_LIST, PARA_RECORD,
    PARA_SELECT, PARA_STRING
)
from flowserv.model.parameter.boolean import Bool
from flowserv.model.parameter.enum import Select
from flowserv.model.parameter.files import File
from flowserv.model.parameter.list import Array
from flowserv.model.parameter.numeric import Int, Float
from flowserv.model.parameter.record import Record
from flowserv.model.parameter.string import String

import flowserv.error as err


"""Dictionary of known parameter types. New types have to be added here."""
PARAMETER_TYPES = {
    PARA_BOOL: Bool,
    PARA_SELECT: Select,
    PARA_FILE: File,
    PARA_FLOAT: Float,
    PARA_INT: Int,
    PARA_LIST: Array,
    PARA_RECORD: Record,
    PARA_STRING: String
}


class ParameterDeserializer(object):
    """Factory for parameter declarations from dictionary serializations. This
    class is merely a dispatcher that looks at the parameter type in a given
    serialization and calls the deserialization method of the respective parameter
    declaration class.
    """
    @staticmethod
    def from_dict(doc: Dict, validate: Optional[bool] = True) -> Parameter:
        """Create instance of parameter declaration class from a given dictionary
        serialization object.

        Parameters
        ----------
        doc: dict
            Dictionary serialization for a parameter declaration.
        validate: bool, default=True
            Validate given serialization if True.

        Returns
        -------
        flowserv.model.parameter.base.Parameter

        Raises
        ------
        flowserv.error.InvalidParameterError
        """
        try:
            cls = PARAMETER_TYPES[doc[TYPE]]
        except KeyError as ex:
            msg = "missing '{}' for {}"
            raise err.InvalidParameterError(msg.format(str(ex), doc))
        return cls.from_dict(doc, validate=validate)
