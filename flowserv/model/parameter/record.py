# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Declarations for record parameter values. Records are collections of
parameters. Each component (field) of a record is identified by a unique name.
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional

from flowserv.model.parameter.base import Parameter, PARA_RECORD

import flowserv.error as err
import flowserv.model.parameter.base as pd
import flowserv.util as util


class Record(Parameter):
    """Record parameter type that associates parameter declarations for record
    components with unique keys.
    """
    def __init__(
        self, name: str, fields: List[Parameter], index: Optional[int] = 0,
        label: Optional[str] = None, help: Optional[str] = None,
        default: Optional[Dict] = None, required: Optional[bool] = False,
        group: Optional[str] = None
    ):
        """Initialize the base properties for a record parameter declaration.

        Parameters
        ----------
        name: string
            Unique parameter identifier
        fields: list
            List of parameter declarations for record fields. The field name is
            the name of the respective parameter.
        index: int, default=0
            Index position of the parameter (for display purposes).
        label: string
            Human-readable parameter name.
        help: string, default=None
            Descriptive text for the parameter.
        default: dict, default=None
            Optional default value.
        required: bool, default=False
            Is required flag.
        group: string, default=None
            Optional identifier for parameter group that this parameter
            belongs to.
        """
        super(Record, self).__init__(
            dtype=PARA_RECORD,
            name=name,
            index=index,
            label=label,
            help=help,
            default=default,
            required=required,
            group=group
        )
        self.fields = dict()
        for para in fields:
            if para.name in self.fields:
                raise err.InvalidParameterError("duplicate field '{}'".format(para.name))
            self.fields[para.name] = para

    def cast(self, value: Any) -> Dict:
        """Convert the given value into a record. Returns a dictionary that is
        a mapping of filed identifier to the converted values returned by the
        respective parameter declaration.

        Expects a list of dictionaries containing two elements: 'name' and
        'value'. The name identifies the record field and the value is the
        argument value for that field.

        Raises an error if the value is not a list, if any of the dictionaries
        are not well-formed, if required fields are not present in the given list,
        or if the respective parameter declaration for a record fields raised an
        exception during cast.

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
        if not isinstance(value, list):
            raise err.InvalidArgumentError('invalid argument type')
        result = dict()
        # Cast all given values using their respective parameter declaration.
        for obj in value:
            util.validate_doc(
                obj,
                mandatory=['name', 'value'],
                exception=err.InvalidArgumentError
            )
            name = obj['name']
            if name not in self.fields:
                raise err.InvalidArgumentError("unknown argument '{}'".format(name))
            result[name] = self.fields[name].cast(obj['value'])
        # Add default values for missing fields.
        for key, para in self.fields.items():
            if key not in result:
                if para.default is not None:
                    result[key] = para.cast(para.default)
                elif para.required:
                    raise err.InvalidArgumentError("missing field '{}'".format(key))
        return result

    @staticmethod
    def from_dict(doc: Dict, validate: Optional[bool] = True) -> Record:
        """Get record parameter instance from a given dictionary serialization.

        Parameters
        ----------
        doc: dict
            Dictionary serialization for record parameter declaration.
        validate: bool, default=True
            Validate the serialized object if True.

        Returns
        -------
        flowserv.model.parameter.record.Record

        Raises
        ------
        flowserv.error.InvalidParameterError
        """
        if validate:
            util.validate_doc(
                doc,
                mandatory=pd.MANDATORY + ['fields'],
                optional=pd.OPTIONAL,
                exception=err.InvalidParameterError
            )
            if doc[pd.TYPE] != PARA_RECORD:
                raise ValueError("invalid type '{}'".format(doc[pd.TYPE]))
        # Deserialize parameter declarations for record fields. Import the
        # deserializer here to avoid cyclic dependencies.
        from flowserv.model.parameter.factory import ParameterDeserializer as deserializer
        fields = list()
        for obj in doc['fields']:
            fields.append(deserializer.from_dict(obj, validate=validate))
        return Record(
            name=doc[pd.NAME],
            fields=fields,
            index=doc[pd.INDEX],
            label=doc.get(pd.LABEL),
            help=doc.get(pd.HELP),
            default=doc.get(pd.DEFAULT),
            required=doc[pd.REQUIRED],
            group=doc.get(pd.GROUP)
        )

    def to_dict(self) -> Dict:
        """Get dictionary serialization for the parameter declaration. Adds
        list of serialized parameter declarations for record fields to the base
        serialization.

        Individual fields are serialized as dictionaries with elements 'name'
        for the field name and 'para' for the serialized parameter declaration.

        Returns
        -------
        dict
        """
        obj = super().to_dict()
        obj['fields'] = [p.to_dict() for p in self.fields.values()]
        return obj
