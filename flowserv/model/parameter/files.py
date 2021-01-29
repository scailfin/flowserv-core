# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Declarations for file parameter values. A file parameter extends the
base parameter class with a target path for the file when creating the workflow
run environment.
"""

from __future__ import annotations
from typing import Dict, IO, Optional, Union

from flowserv.model.files.base import IOHandle
from flowserv.model.parameter.base import Parameter, PARA_FILE

import flowserv.error as err
import flowserv.model.parameter.base as pd
import flowserv.util as util


class File(Parameter):
    """File parameter type. Extends the base parameter with a target path for
    the file.
    """
    def __init__(
        self, name: str, index: Optional[int] = 0, target: str = None,
        label: Optional[str] = None, help: Optional[str] = None,
        default: Optional[bool] = None, required: Optional[bool] = False,
        group: Optional[str] = None
    ):
        """Initialize the base properties a enumeration parameter declaration.

        Parameters
        ----------
        name: string
            Unique parameter identifier
        index: int, default=0
            Index position of the parameter (for display purposes).
        target: string, default=None
            Target path for the file when creating the workflow run
            environment.
        label: string, default=None
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
        super(File, self).__init__(
            dtype=PARA_FILE,
            name=name,
            index=index,
            label=label,
            help=help,
            default=default,
            required=required,
            group=group
        )
        self.target = target

    def cast(self, value: IOHandle, target: str = None):
        """Get an instance of the InputFile class for a given argument value.
        The input value can either be a string (filename) or a dictionary.

        Parameters
        ----------
        value: flowserv.model.files.base.IOHandle
            Handle for a file object.
        target: string, default=None
            Optional user-provided target path. If None the defined target path
            is used or the defined default value. If neither is given an error
            is raised.

        Returns
        -------
        flowserv.model.parameter.files.InputFile

        Raises
        ------
        flowserv.error.InvalidArgumentError
        flowserv.error.UnknownFileError
        """
        # Ensure that the target path is set.
        if target is None:
            if self.target is not None:
                target = self.target
            elif self.default is not None:
                target = self.default
            else:
                raise err.InvalidArgumentError('missing target path')
        # The InputFile constructor may raise a TypeError if the source
        # argument is not a string.
        try:
            return InputFile(source=value, target=target)
        except TypeError as ex:
            raise err.InvalidArgumentError(str(ex))

    @staticmethod
    def from_dict(doc: Dict, validate: bool = True) -> File:
        """Get file parameter instance from a dictionary serialization.

        Parameters
        ----------
        doc: dict
            Dictionary serialization for file parameter declaration.
        validate: bool, default=True
            Validate the serialized object if True.

        Returns
        -------
        flowserv.model.parameter.files.File

        Raises
        ------
        flowserv.error.InvalidParameterError
        """
        if validate:
            util.validate_doc(
                doc,
                mandatory=pd.MANDATORY,
                optional=pd.OPTIONAL + ['target'],
                exception=err.InvalidParameterError
            )
            if doc[pd.TYPE] != PARA_FILE:
                raise ValueError("invalid type '{}'".format(doc[pd.TYPE]))
        return File(
            name=doc[pd.NAME],
            index=doc[pd.INDEX],
            label=doc.get(pd.LABEL),
            help=doc.get(pd.HELP),
            default=doc.get(pd.DEFAULT),
            required=doc[pd.REQUIRED],
            group=doc.get(pd.GROUP),
            target=doc.get('target')
        )

    def to_dict(self) -> Dict:
        """Get dictionary serialization for the parameter declaration. Adds
        target path to the base serialization.

        Returns
        -------
        dict
        """
        obj = super().to_dict()
        obj['target'] = self.target
        return obj


class InputFile(object):
    """The InputFile represents the value for a template parameter of type
    'file'. This class contains the path to the source for an uploaded file as
    well as the target path for the Upload.
    """
    def __init__(self, source: IOHandle, target: str):
        """Initialize the object properties.

        Parameters
        ----------
        source: flowserv.model.files.base.IOHandle
            Handle for a file object.
        target: string
            Relative target path for file upload.

        Raises
        ------
        TypeError
        """
        if not isinstance(source, IOHandle):
            raise TypeError("invalid source type '{}'".format(type(source)))
        self._source = source
        self._target = target

    def __str__(self):
        """The string representation of an input file is the path to the target
        file. This is important since the parameter replacement function
        converts input arguments to string using str().

        Returns
        -------
        string
        """
        return self._target

    def source(self) -> Union[str, IO]:
        """Get the source path for the file.

        Returns
        -------
        flowserv.model.files.base.IOHandle
        """
        return self._source

    def target(self) -> str:
        """Get the target path for the file.

        Returns
        -------
        string
        """
        return self._target
