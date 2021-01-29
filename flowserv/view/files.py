# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Serializer for uploaded workflow user group files."""

from typing import Dict, List

from flowserv.model.base import FileObject


"""Serialization labels."""
FILE_DATE = 'createdAt'
FILE_ID = 'id'
FILE_LIST = 'files'
FILE_NAME = 'name'
FILE_SIZE = 'size'


class UploadFileSerializer():
    """Default serializer for handles and listings of files that were uploaded
    for a workflow groups."""
    def file_handle(self, group_id: str, fh: FileObject) -> Dict:
        """Get serialization for a file handle.

        Parameters
        ----------
        group_id: string
            Unique workflow group identifier
        fh: flowserv.model.base.FileObject
            File handle

        Returns
        -------
        dict
        """
        return {
            FILE_ID: fh.file_id,
            FILE_NAME: fh.name,
            FILE_DATE: fh.created_at,
            FILE_SIZE: fh.size
        }

    def file_listing(self, group_id: str, files: List[FileObject]) -> Dict:
        """Get serialization for listing of uploaded files for a given
        workflow group.

        Parameters
        ----------
        group_id: string
            Unique workflow group identifier
        files: list(flowserv.model.base.FileObject)
            List of file handle

        Returns
        -------
        dict
        """
        return {FILE_LIST: [self.file_handle(group_id, fh) for fh in files]}
