# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""The workflow user group files API component provides methods to access,
delete, and upload files for workflow groups.
"""

from typing import Dict, IO, Optional

from flowserv.model.auth import Auth
from flowserv.model.files.base import FileHandle, IOHandle
from flowserv.model.group import WorkflowGroupManager
from flowserv.service.files.base import UploadFileService
from flowserv.view.files import UploadFileSerializer
import flowserv.error as err


class LocalUploadFileService(UploadFileService):
    """API component that provides methods to access, delete and upload files
    for workflow user groups.
    """
    def __init__(
        self, group_manager: WorkflowGroupManager, auth: Auth,
        user_id: Optional[str] = None,
        serializer: Optional[UploadFileSerializer] = None
    ):
        """Initialize the internal reference to the workflow group manager and
        to resource serializer.

        Parameters
        ----------
        group_manager: flowserv.model.group.WorkflowGroupManager
            Manager for workflow groups
        auth: flowserv.model.auth.Auth
            Implementation of the authorization policy for the API
        user_id: string, default=None
            Identifier of an authenticated user.
        serializer: flowserv.view.files.UploadFileSerializer, default=None
            Resource serializer
        """
        self.group_manager = group_manager
        self.auth = auth
        self.user_id = user_id
        self.serialize = serializer if serializer is not None else UploadFileSerializer()

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
        # Raise an error if the user does not have rights to delete files for
        # the workflow group or if the workflow group does not exist.
        if not self.auth.is_group_member(group_id=group_id, user_id=self.user_id):
            raise err.UnauthorizedAccessError()
        # Delete the file using the workflow group handle
        self.group_manager.delete_file(group_id=group_id, file_id=file_id)

    def get_uploaded_file(self, group_id: str, file_id: str) -> IO:
        """Get IO buffer for file with given identifier that was uploaded to the
        workflow group.

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
        io.BytesIO

        Raises
        ------
        flowserv.error.UnauthorizedAccessError
        flowserv.error.UnknownFileError
        flowserv.error.UnknownWorkflowGroupError
        """
        fh = self.get_uploaded_file_handle(group_id=group_id, file_id=file_id)
        return fh.open()

    def get_uploaded_file_handle(self, group_id: str, file_id: str) -> FileHandle:
        """Get handle for file with given identifier that was uploaded to the
        workflow group.

        Returns the file handle for the uploaded file.

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
        flowserv.model.base.files.FileHandle

        Raises
        ------
        flowserv.error.UnauthorizedAccessError
        flowserv.error.UnknownFileError
        flowserv.error.UnknownWorkflowGroupError
        """
        # Raise an error if the user does not have rights to access files for
        # the workflow group or if the workflow group does not exist (only if
        # the user identifier is given).
        if self.user_id is not None:
            is_member = self.auth.is_group_member(
                group_id=group_id,
                user_id=self.user_id
            )
            if not is_member:
                raise err.UnauthorizedAccessError()
        # Return the file handle.
        return self.group_manager.get_uploaded_file(
            group_id=group_id,
            file_id=file_id
        )

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
        # Raise an error if the user does not have rights to access files for
        # the workflow group or if the workflow group does not exist.
        if not self.auth.is_group_member(group_id=group_id, user_id=self.user_id):
            raise err.UnauthorizedAccessError()
        return self.serialize.file_listing(
            group_id=group_id,
            files=self.group_manager.list_uploaded_files(group_id)
        )

    def upload_file(self, group_id: str, file: IOHandle, name: str) -> Dict:
        """Create a file for a given workflow group.

        Parameters
        ----------
        group_id: string
            Unique workflow group identifier
        file: fflowserv.model.files.base.IOHandle
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
        # Raise an error if the user does not have rights to upload files for
        # the workflow group or if the workflow group does not exist.
        if not self.auth.is_group_member(group_id=group_id, user_id=self.user_id):
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
