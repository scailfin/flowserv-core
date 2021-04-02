# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Base classes for workflow runtime storage volumes."""

from abc import ABCMeta, abstractmethod
from typing import Optional, Tuple

from flowserv.model.files.base import IOHandle

import flowserv.util as util


class StorageVolume(metaclass=ABCMeta):
    """The runtime storage volume provides access to a file system-like object
    for storing and retrieving files and folders that are required or produced
    by a workflow step.

    Storage volumes are used to provide a copy of the required run files for
    a workflow step. Each valume has a unique identifier that is used to
    keep track which files and file versions are available in the volume.
    """
    def __init__(self, identifier: Optional[str] = None):
        """Initialize the unique volume identifier.

        If no identifier is provided a unique identifier is generated.

        Parameters
        ----------
        identifier: string
            Unique identifier.
        """
        self.identifier = identifier if identifier is not None else util.get_unique_identifier()

    @abstractmethod
    def close(self):
        """Close any open connection and release all resources when workflow
        execution is done.
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def download(self, src: str, dst: str):
        """Download the file or folder at the source path to the given
        destination.

        The source path is relative to the base directory for the workflow run.
        The destination path is absolute or relative to the current working
        directory.

        Parameters
        ----------
        src: string
            Relative source path on the environment run directory.
        dst: string
            Absolute or relative path on the local file system.
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def erase(self):
        """Erase the storage volume base directory and all its contents."""
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def upload(self, src: Tuple[IOHandle, str], dst: str):
        """Upload a file or folder to the storage volume.

        The destination is relative to the base directory of the run
        environment.

        Parameters
        ----------
        src: string or flowserv.model.files.base.IOHandle
            Source file or folder that is being uploaded to the storage volume.
        dst: string
            Relative target path for the uploaded files.
        """
        raise NotImplementedError()  # pragma: no cover
