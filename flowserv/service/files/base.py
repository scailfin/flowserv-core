# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Interface for the workflow user group files API component that defines
methods to access, delete, and upload files for workflow groups.
"""

from abc import ABCMeta, abstractmethod
from typing import Dict, IO

from flowserv.model.files.base import FileObject

from flowserv.config.base import get_variable
from flowserv.model.files.base import FileStore

import flowserv.config.files as config
import flowserv.error as err


class UploadFileService(metaclass=ABCMeta):
    """API component that provides methods to access, delete and upload files
    for workflow user groups.
    """
    @abstractmethod
    def delete_file(self, group_id: str, file_id: str):
        """Delete file with given identifier that was previously uploaded.

        Raises errors if the file or the workflow group does not exist or if
        the user is not authorized to delete the file.

        Parameters
        ----------
        group_id: string
            Unique workflow group identifier
        file_id: string
            Unique file identifier

        Raises
        ------
        flowserv.error.UnauthorizedAccessError
        flowserv.error.UnknownFileError
        """
        raise NotImplementedError()

    @abstractmethod
    def get_uploaded_file(self, group_id: str, file_id: str) -> IO:
        """Get handle for file with given identifier that was uploaded to the
        workflow group.

        Currently we do allow downloads for non-submission members (i.e., the
        user identifier is optional).

        Parameters
        ----------
        group_id: string
            Unique workflow group identifier
        file_id: string
            Unique file identifier

        Returns
        -------
        io.BytesIO

        Raises
        ------
        flowserv.error.UnauthorizedAccessError
        flowserv.error.UnknownFileError
        flowserv.error.UnknownWorkflowGroupError
        """
        raise NotImplementedError()

    @abstractmethod
    def list_uploaded_files(self, group_id: str) -> Dict:
        """Get a listing of all files that have been uploaded for the given
        workflow group.

        Parameters
        ----------
        group_id: string
            Unique workflow group identifier

        Returns
        -------
        dict

        Raises
        ------
        flowserv.error.UnauthorizedAccessError
        flowserv.error.UnknownWorkflowGroupError
        """
        raise NotImplementedError()

    @abstractmethod
    def upload_file(self, group_id: str, file: FileObject, name: str) -> Dict:
        """Create a file for a given workflow group.

        Parameters
        ----------
        group_id: string
            Unique workflow group identifier
        file: fflowserv.model.files.base.FileObject
            File object (e.g., uploaded via HTTP request)
        name: string
            Name of the file

        Returns
        -------
        dict

        Raises
        ------
        flowserv.error.ConstraintViolationError
        flowserv.error.UnauthorizedAccessError
        flowserv.error.UnknownWorkflowGroupError
        """
        raise NotImplementedError()


# -- Factory pattern for file stores ------------------------------------------

def get_filestore(raise_error: bool = True) -> FileStore:
    """Factory pattern to create file store instances for the service API. Uses
    the environment variables FLOWSERV_FILESTORE_MODULE and
    FLOWSERV_FILESTORE_CLASS to create an instance of the file store. If the
    environment variables are not set the FileSystemStore is returned as the
    default file store.

    Parameters
    ----------
    raise_error: bool, default=True
        Flag to indicate whether an error is raised if a value for a
        configuration variable is missing or not.

    Returns
    -------
    flowserv.model.files.base.FileStore
    """
    module_name = get_variable(name=config.FLOWSERV_FILESTORE_MODULE)
    class_name = get_variable(name=config.FLOWSERV_FILESTORE_CLASS)
    # If both environment variables are None return the default file store.
    # Otherwise, import the specified module and return an instance of the
    # controller class. An error is raised if only one of the two environment
    # variables is set.
    if module_name is None and class_name is None:
        from flowserv.model.files.fs import FileSystemStore
        return FileSystemStore()
    elif module_name is not None and class_name is not None:
        from importlib import import_module
        module = import_module(module_name)
        return getattr(module, class_name)()
    elif module_name is None and raise_error:
        raise err.MissingConfigurationError(config.FLOWSERV_FILESTORE_MODULE)
    elif raise_error:
        raise err.MissingConfigurationError(config.FLOWSERV_FILESTORE_CLASS)
