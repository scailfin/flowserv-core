# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Declarations for file parameter values. Each file parameter extends the
base parameter class with a target path for the file when creating the workflow
run environment.
"""

from typing import Dict, IO, Union

from flowserv.model.files.base import FileObject
from flowserv.model.parameter.base import ParameterBase

import flowserv.error as err
import flowserv.model.parameter.base as pd
import flowserv.util as util


"""Unique parameter type identifier."""
PARA_FILE = 'file'


class FileParameter(ParameterBase):
    """File parameter type. Extends the base parameter with a target path for
    the file.
    """
    def __init__(
        self, para_id: str, name: str, index: int, target: str = None,
        description: str = None, default_value: str = None,
        is_required: bool = False, module_id: str = None
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
        target: string, default=None
            Target path for the file when creating the workflow run
            environment.
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
        super(FileParameter, self).__init__(
            para_id=para_id,
            type_id=PARA_FILE,
            name=name,
            index=index,
            description=description,
            default_value=default_value,
            is_required=is_required,
            module_id=module_id
        )
        self.target = target

    @classmethod
    def from_dict(cls, doc: Dict, validate: bool = True):
        """Get enumeration parameter instance from dictionary serialization.

        Parameters
        ----------
        doc: dict
            Dictionary serialization for file parameter.
        validate: bool, default=True
            Validate the serialized object if True.

        Returns
        -------
        flowserv.model.parameter.files.FileParameter

        Raises
        ------
        flowserv.error.InvalidParameterError
        """
        if validate:
            try:
                util.validate_doc(
                    doc,
                    mandatory=[pd.ID, pd.TYPE, pd.NAME, pd.INDEX, pd.REQUIRED],
                    optional=[pd.DESC, pd.DEFAULT, pd.MODULE, 'target']
                )
            except ValueError as ex:
                raise err.InvalidParameterError(str(ex))
            if doc[pd.TYPE] != PARA_FILE:
                raise ValueError("invalid type '{}'".format(doc[pd.TYPE]))
        return cls(
            para_id=doc[pd.ID],
            name=doc[pd.NAME],
            index=doc[pd.INDEX],
            description=doc.get(pd.DESC),
            default_value=doc.get(pd.DEFAULT),
            is_required=doc[pd.REQUIRED],
            module_id=doc.get(pd.MODULE),
            target=doc.get('target')
        )

    def to_argument(self, value: FileObject, target: str = None):
        """Get an instance of the InputFile class for a given argument value.
        The input value can either be a string (filename) or a dictionary.

        Parameters
        ----------
        value: flowserv.model.files.base.FileObject
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
            elif self.default_value is not None:
                target = self.default_value
            else:
                raise err.InvalidArgumentError('missing target path')
        # The InputFile constructor may raise a TypeError if the source
        # argument is not a string.
        try:
            return InputFile(source=value, target=target)
        except TypeError as ex:
            raise err.InvalidArgumentError(str(ex))

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
    'file'. This class contains the path to the sourcefor an uploaded file as
    well as the target path for the Upload.
    """
    def __init__(self, source: FileObject, target: str):
        """Initialize the object properties.

        Parameters
        ----------
        source: flowserv.model.files.base.FileObject
            Handle for a file object.
        target: string
            Relative target path for file upload.

        Raises
        ------
        TypeError
        """
        if not isinstance(source, FileObject):
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
        flowserv.model.files.base.FileObject
        """
        return self._source

    def target(self) -> str:
        """Get the target path for the file.

        Returns
        -------
        string
        """
        return self._target


# -- Helper Methods -----------------------------------------------------------

def is_file(para: ParameterBase) -> bool:
    """Test if the given parameter is of type PARA_FILE.

    Parameters
    ----------
    para: flowserv.model.parameter.base.ParameterBase
        Template parameter definition.

    Returns
    -------
    bool
    """
    return para.type_id == PARA_FILE
