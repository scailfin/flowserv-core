# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Serializer for uploaded workflow user group files."""

from flowserv.view.base import Serializer


class UploadFileSerializer(Serializer):
    """Default serializer for handles and= listings of files that were uploaded
    for a workflow groups."""
    def __init__(self, labels=None):
        """Initialize serialization labels.

        Parameters
        ----------
        labels: object, optional
            Object instance that contains the values for serialization labels
        """
        super(UploadFileSerializer, self).__init__(
            labels={
                'FILE_DATE': 'createdAt',
                'FILE_ID': 'id',
                'FILE_LIST': 'files',
                'FILE_NAME': 'name',
                'FILE_SIZE': 'size'
            },
            override_labels=labels
        )

    def file_handle(self, group_id, fh):
        """Get serialization for a file handle.

        Parameters
        ----------
        group_id: string
            Unique workflow group identifier
        fh: flowserv.model.base.FileHandle
            File handle

        Returns
        -------
        dict
        """
        LABELS = self.labels
        return {
            LABELS['FILE_ID']: fh.file_id,
            LABELS['FILE_NAME']: fh.name,
            LABELS['FILE_DATE']: fh.created_at,
            LABELS['FILE_SIZE']: fh.size
        }

    def file_listing(self, group_id, files):
        """Get serialization for listing of uploaded files for a given
        workflow group.

        Parameters
        ----------
        group_id: string
            Unique workflow group identifier
        files: list(flowserv.model.base.FileHandle)
            List of file handle

        Returns
        -------
        dict
        """
        FILES = self.labels['FILE_LIST']
        return {FILES: [self.file_handle(group_id, fh) for fh in files]}
