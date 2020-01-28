# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""The workflow user group files API component provides methods to access,
delete, and upload files for workflow groups.
"""

from flowserv.view.files import UploadFileSerializer

import flowserv.core.error as err


class UploadFileService(object):
    """API component that provides methods to access, delete and upload files
    for workflow user groups.
    """
    def __init__(self, group_manager, auth, urls, serializer=None):
        """Initialize the internal reference to the workflow group manager and
        to the route factory.

        Parameters
        ----------
        group_manager: flowserv.model.group.manager.GroupManager
            Manager for workflow groups
        auth: flowserv.model.user.auth.Auth
            Implementation of the authorization policy for the API
        urls: flowserv.view.route.UrlFactory
            Factory for API resource Urls
        serializer: flowserv.view.files.UploadFileSerializer, optional
            Override the default serializer
        """
        self.group_manager = group_manager
        self.auth = auth
        self.urls = urls
        self.serialize = serializer
        if self.serialize is None:
            self.serialize = UploadFileSerializer(self.urls)

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
        flowserv.core.error.UnauthorizedAccessError
        flowserv.core.error.UnknownFileError
        """
        # Raise an error if the user does not have rights to delete files for
        # the workflow group or if the workflow group does not exist.
        if not self.auth.is_group_member(group_id=group_id, user_id=user_id):
            raise err.UnauthorizedAccessError()
        # Delete the file using the workflow group handle
        self.group_manager.get_group(group_id).delete_file(file_id)

    def get_file(self, group_id, file_id, user_id):
        """Get handle for file with given identifier that was uploaded to the
        workflow group.

        Returns the file handle and the serialization of the file handle.

        Parameters
        ----------
        group_id: string
            Unique workflow group identifier
        file_id: string
            Unique file identifier
        user_id: string
            Unique user identifier

        Returns
        -------
        flowserv.core.files.FileHandle, dict

        Raises
        ------
        flowserv.core.error.UnauthorizedAccessError
        flowserv.core.error.UnknownFileError
        flowserv.core.error.UnknownWorkflowGroupError
        """
        # Raise an error if the user does not have rights to access files for
        # the workflow group or if the workflow group does not exist.
        if not self.auth.is_group_member(group_id=group_id, user_id=user_id):
            raise err.UnauthorizedAccessError()
        # Return the file handle and a serialization of tit
        fh = self.group_manager.get_group(group_id).get_file(file_id)
        doc = self.serialize.file_handle(group_id=group_id, fh=fh)
        return fh, doc

    def list_files(self, group_id, user_id):
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
        flowserv.core.error.UnauthorizedAccessError
        flowserv.core.error.UnknownWorkflowGroupError
        """
        # Raise an error if the user does not have rights to access files for
        # the workflow group or if the workflow group does not exist.
        if not self.auth.is_group_member(group_id=group_id, user_id=user_id):
            raise err.UnauthorizedAccessError()
        return self.serialize.file_listing(
            group_id=group_id,
            files=self.group_manager.get_group(group_id).list_files()
        )

    def upload_file(self, group_id, file, name, user_id, file_type=None):
        """Create a file for a given workflow group.

        Parameters
        ----------
        group_id: string
            Unique workflow group identifier
        file: werkzeug.datastructures.FileStorage
            File object (e.g., uploaded via HTTP request)
        name: string
            Name of the file
        user_id: string
            Unique user identifier
        file_type: string, optional
            Identifier for the file type (e.g., the file MimeType). This could
            also by the identifier of a content handler.

        Returns
        -------
        dict

        Raises
        ------
        flowserv.core.error.ConstraintViolationError
        flowserv.core.error.UnauthorizedAccessError
        flowserv.core.error.UnknownWorkflowGroupError
        """
        # Raise an error if the user does not have rights to upload files for
        # the workflow group or if the workflow group does not exist.
        if not self.auth.is_group_member(group_id=group_id, user_id=user_id):
            raise err.UnauthorizedAccessError()
        # Return serialization of the uploaded file
        fh = self.group_manager.get_group(group_id).upload_file(
            file=file,
            name=name,
            file_type=file_type
        )
        return self.serialize.file_handle(group_id=group_id, fh=fh)
