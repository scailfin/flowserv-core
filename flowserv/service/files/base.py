# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Interface for the workflow user group files API component that defines
methods to access, delete, and upload files for workflow groups.
"""

from abc import ABCMeta, abstractmethod
from typing import Dict, IO

from flowserv.volume.base import IOHandle


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
        raise NotImplementedError()  # pragma: no cover

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
        raise NotImplementedError()  # pragma: no cover

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
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def upload_file(self, group_id: str, file: IOHandle, name: str) -> Dict:
        """Create a file for a given workflow group.

        Parameters
        ----------
        group_id: string
            Unique workflow group identifier
        file: flowserv.volume.base.IOHandle
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
        raise NotImplementedError()  # pragma: no cover
