# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""API service component that provides functionality to access, delete, and
upload files at a remote RESTful API.
"""

from typing import Dict, IO

from flowserv.model.files.base import IOHandle
from flowserv.service.descriptor import ServiceDescriptor
from flowserv.service.files.base import UploadFileService
from flowserv.service.remote import delete, download_file, get, post

import flowserv.service.descriptor as route


class RemoteUploadFileService(UploadFileService):
    """API component that provides methods to access, delete and upload files
    for workflow user groups at a remote RESTful API.
    """
    def __init__(self, descriptor: ServiceDescriptor):
        """Initialize the Url route patterns from the service descriptor.

        Parameters
        ----------
        descriptor: flowserv.service.descriptor.ServiceDescriptor
            Service descriptor containing the API route patterns.
        """
        # Short cut to access urls from the descriptor.
        self.urls = descriptor.urls

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
        url = self.urls(route.FILES_DELETE, userGroupId=group_id, fileId=file_id)
        return delete(url=url)

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
        url = self.urls(route.FILES_DOWNLOAD, userGroupId=group_id, fileId=file_id)
        return download_file(url=url)

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
        return get(url=self.urls(route.FILES_LIST, userGroupId=group_id))

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
        files = {'files': (name, file.open())}
        url = self.urls(route.FILES_UPLOAD, userGroupId=group_id)
        return post(url=url, files=files)
