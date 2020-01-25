# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Serializer for uploaded workflow user group files."""

import flowserv.view.hateoas as hateoas
import flowserv.view.labels as labels


class UploadFileSerializer(object):
    """Default serializer for handles and= listings of files that were uploaded
    for a workflow groups."""
    def __init__(self, urls):
        """Initialize the reference to the Url factory.

        Parameters
        ----------
        urls: flowserv.view.route.UrlFactory
            Factory for resource urls
        """
        self.urls = urls

    def file_handle(self, group_id, fh):
        """Get serialization for a file handle.

        Parameters
        ----------
        group_id: string
            Unique workflow group identifier
        fh: flowserv.core.files.FileHandle
            File handle

        Returns
        -------
        dict
        """
        del_url = self.urls.delete_file(group_id, fh.identifier)
        dwnld_url = self.urls.download_file(group_id, fh.identifier)
        return {
            labels.ID: fh.identifier,
            labels.NAME: fh.name,
            labels.CREATED_AT: fh.created_at.isoformat(),
            labels.FILESIZE: fh.size,
            labels.LINKS: hateoas.serialize({
                hateoas.action(hateoas.DOWNLOAD): dwnld_url,
                hateoas.action(hateoas.DELETE): del_url
            })
        }

    def file_listing(self, group_id, files):
        """Get serialization for listing of uploaded files for a given
        workflow group.

        Parameters
        ----------
        group_id: string
            Unique workflow group identifier
        files: list(flowserv.core.files.FileHandle)
            List of file handle

        Returns
        -------
        dict
        """
        return {
            labels.FILES: [self.file_handle(group_id, fh) for fh in files],
            labels.LINKS: hateoas.serialize({
                hateoas.SELF: self.urls.list_files(group_id)
            })
        }
