# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""The workflow user group files API component provides methods to access,
delete, and upload files for workflow groups.
"""

from flowserv.config.base import get_variable
from flowserv.model.files.base import FileObject, FileStore

import flowserv.config.files as config
import flowserv.error as err


class UploadFileService(object):
    """API component that provides methods to access, delete and upload files
    for workflow user groups.
    """
    def __init__(self, group_manager, auth, serializer):
        """Initialize the internal reference to the workflow group manager and
        to resource serializer.

        Parameters
        ----------
        group_manager: flowserv.model.group..GroupManager
            Manager for workflow groups
        auth: flowserv.model.auth.Auth
            Implementation of the authorization policy for the API
        serializer: flowserv.view.files.UploadFileSerializer
            Resource serializer
        """
        self.group_manager = group_manager
        self.auth = auth
        self.serialize = serializer

    def delete_file(self, group_id, file_id, user_id):
        """Delete file with given identifier that was previously uploaded.

        Raises errors if the file or the workflow group does not exist or if
        the user is not authorized to delete the file.

        Parameters
        ----------
        group_id: string
            Unique workflow group identifier
        file_id: string
            Unique file identifier
        user_id: string
            Unique user identifier

        Raises
        ------
        flowserv.error.UnauthorizedAccessError
        flowserv.error.UnknownFileError
        """
        # Raise an error if the user does not have rights to delete files for
        # the workflow group or if the workflow group does not exist.
        if not self.auth.is_group_member(group_id=group_id, user_id=user_id):
            raise err.UnauthorizedAccessError()
        # Delete the file using the workflow group handle
        self.group_manager.delete_file(group_id=group_id, file_id=file_id)

    def get_uploaded_file(self, group_id, file_id, user_id=None):
        """Get handle for file with given identifier that was uploaded to the
        workflow group.

        Currently we do allow downloads for non-submission members (i.e., the
        user identifier iis optional). If a user identifier is given
        Returns the file handle and the serialization of the file handle.

        Parameters
        ----------
        group_id: string
            Unique workflow group identifier
        file_id: string
            Unique file identifier
        user_id: string, optional
            Unique user identifier

        Returns
        -------
        flowserv.model.files.base.DatabaseFile

        Raises
        ------
        flowserv.error.UnauthorizedAccessError
        flowserv.error.UnknownFileError
        flowserv.error.UnknownWorkflowGroupError
        """
        # Raise an error if the user does not have rights to access files for
        # the workflow group or if the workflow group does not exist (only if
        # the user identifier is given).
        if user_id is not None:
            is_member = self.auth.is_group_member(
                group_id=group_id,
                user_id=user_id
            )
            if not is_member:
                raise err.UnauthorizedAccessError()
        # Return the file handle.
        return self.group_manager.get_uploaded_file(
            group_id=group_id,
            file_id=file_id
        )

    def list_uploaded_files(self, group_id, user_id):
        """Get a listing of all files that have been uploaded for the given
        workflow group.

        Parameters
        ----------
        group_id: string
            Unique workflow group identifier
        user_id: string
            unique user identifier

        Returns
        -------
        dict

        Raises
        ------
        flowserv.error.UnauthorizedAccessError
        flowserv.error.UnknownWorkflowGroupError
        """
        # Raise an error if the user does not have rights to access files for
        # the workflow group or if the workflow group does not exist.
        if not self.auth.is_group_member(group_id=group_id, user_id=user_id):
            raise err.UnauthorizedAccessError()
        return self.serialize.file_listing(
            group_id=group_id,
            files=self.group_manager.list_uploaded_files(group_id)
        )

    def upload_file(
        self, group_id: str, file: FileObject, name: str, user_id: str
    ):
        """Create a file for a given workflow group.

        Parameters
        ----------
        group_id: string
            Unique workflow group identifier
        file: fflowserv.model.files.base.FileObject
            File object (e.g., uploaded via HTTP request)
        name: string
            Name of the file
        user_id: string
            Unique user identifier

        Returns
        -------
        dict

        Raises
        ------
        flowserv.error.ConstraintViolationError
        flowserv.error.UnauthorizedAccessError
        flowserv.error.UnknownWorkflowGroupError
        """
        # Raise an error if the user does not have rights to upload files for
        # the workflow group or if the workflow group does not exist.
        if not self.auth.is_group_member(group_id=group_id, user_id=user_id):
            raise err.UnauthorizedAccessError()
        # Convert the uploaded FileStorage object into a bytes buffer before
        # passing it to the group manager. Return serialization of the handle
        # for the uploaded file.
        fh = self.group_manager.upload_file(
            group_id=group_id,
            file=file,
            name=name
        )
        return self.serialize.file_handle(group_id=group_id, fh=fh)


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
