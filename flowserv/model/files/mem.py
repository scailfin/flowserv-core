# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Implementation of the :class:`flowserv.model.files.bucket.Bucket` interface
that maintains files as IO objects in a memory cache. This implementatio is
primarily intended for test purposes.
"""

from typing import IO, Iterable, List

from flowserv.model.files.bucket import Bucket

import flowserv.error as err


class MemBucket(Bucket):
    """Implementation of the :class:`flowserv.model.files.bucket.Bucket` interface
    for test purposes. The test bucket maintains all files in memory.
    """
    def __init__(self):
        """Initialize the file cache."""
        self.objects = dict()

    def delete(self, keys: Iterable[str]):
        """Delete files that are identified by their relative path in the given
        list of object keys.

        Parameters
        ----------
        keys: iterable of string
            Unique identifier for objects that are being deleted.
        """
        for key in keys:
            if key in self.objects:
                del self.objects[key]

    def download(self, key: str) -> IO:
        """Read file content into an IO buffer.

        Parameters
        ----------
        key: string
            Unique object identifier.

        Returns
        -------
        io.BytesIO
        """
        if key in self.objects:
            data = self.objects[key]
            data.seek(0)
            return data
        raise err.UnknownFileError(key)

    def query(self, filter: str) -> List[str]:
        """Return all files with relative paths that match the given query
        prefix.

        Parameters
        ----------
        filter: str
            Prefix query for object identifiers.

        Returns
        -------
        list of string
        """
        return [key for key in self.objects if key.startswith(filter)]

    def upload(self, file: IO, key: str):
        """Store the given IO object on the file system.

        Parameters
        ----------
        file: flowserv.model.files.base.IOHandle
            Handle for uploaded file.
        key: string
            Unique object identifier.
        """
        self.objects[key] = file.open()
