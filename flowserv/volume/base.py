# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Base classes for workflow runtime storage volumes."""

from __future__ import annotations
from abc import ABCMeta, abstractmethod
from typing import IO, List, Optional, Tuple

import flowserv.util as util


# -- File objects -------------------------------------------------------------

class IOHandle(metaclass=ABCMeta):
    """Wrapper around different file objects (i.e., files on disk or files in
    object stores). Provides functionality to load file content as a bytes
    buffer and to write file contents to disk.
    """
    @abstractmethod
    def open(self) -> IO:
        """Get file contents as a BytesIO buffer.

        Returns
        -------
        io.BytesIO

        Raises
        ------
        flowserv.error.UnknownFileError
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def size(self) -> int:
        """Get size of the file in the number of bytes.

        Returns
        -------
        int
        """
        raise NotImplementedError()  # pragma: no cover


class IOBuffer(IOHandle):
    """Implementation of the file object interface for bytes IO buffers."""
    def __init__(self, buf: IO):
        """Initialize the IO buffer.

        Parameters
        ----------
        buf: io.BytesIO
            IO buffer containing the file contents.
        """
        self.buf = buf

    def open(self) -> IO:
        """Get the associated BytesIO buffer.

        Returns
        -------
        io.BytesIO
        """
        self.buf.seek(0)
        return self.buf

    def size(self) -> int:
        """Get size of the file in the number of bytes.

        Returns
        -------
        int
        """
        return self.buf.getbuffer().nbytes


# -- Storage volumes ----------------------------------------------------------

class StorageVolume(metaclass=ABCMeta):
    """The runtime storage volume provides access to a file system-like object
    for storing and retrieving files and folders that are required or produced
    by a workflow step.

    Storage volumes are used to provide a copy of the required run files for
    a workflow step. Each volume has a unique identifier that is used to
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

    def download(self, src: str, store: StorageVolume):
        """Download the file or folder at the source path of this storage
        volume to the given storage volume.

        The source path is relative to the base directory for the workflow run.

        Parameters
        ----------
        src: string
            Relative source path on the environment run directory.
        store: flowserv.volume.base.StorageValue
            Storage volume for destination files.
        """
        copy_files(path=src, source=self, target=store)

    @abstractmethod
    def erase(self):
        """Erase the storage volume base directory and all its contents."""
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def load(self, key: str) -> IOHandle:
        """Load a file object at the source path of this volume store.

        Returns a file handle that can be used to open and read the file.

        Parameters
        ----------
        key: str
            Path to a file object in the storage volume.

        Returns
        --------
        flowserv.volume.base.IOHandle
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def store(self, file: IOHandle, dst: str):
        """Store a given file object at the destination path of this volume
        store.

        Parameters
        ----------
        file: flowserv.volume.base.IOHandle
            File-like object that is being stored.
        dst: str
            Destination path for the stored object.
        """
        raise NotImplementedError()  # pragma: no cover

    def upload(self, src: str, store: StorageVolume):
        """Upload a file or folder from the src path of the given storage
        volume to this storage volume.

        Parameters
        ----------
        src: string or flowserv.model.files.base.IOHandle
            Source file or folder that is being uploaded to the storage volume.
        store: flowserv.volume.base.StorageValue
            Storage volume for source files.
        """
        copy_files(path=src, source=store, target=self)

    @abstractmethod
    def walk(self, src: str) -> List[Tuple[str, IOHandle]]:
        """Get list of all files at the given source path.

        If the source path references a single file the returned list will
        contain a single entry. If the source specifies a folder the result
        contains a list of all files in that folder and the subfolders.

        Parameters
        ----------
        src: str
            Source path specifying a file or folder.

        Returns
        -------
        list of tuples (str, flowserv.volume.base.IOHandle)
        """
        raise NotImplementedError()  # pragma: no cover


# -- Helper Functions ---------------------------------------------------------

def copy_files(path: str, source: StorageVolume, target: StorageVolume):
    """Copy files and folders at the source path (path) of a given source
    storage volume to the destination path (path) of a target storage volume.

    Parameters
    ----------
    path: str
        Path specifying the source file or folder. This path is also the target
        path for the copied file(s).
    source: flowserv.volume.base.StorageValue
        Storage volume for source files.
    target: flowserv.volume.base.StorageValue
        Storage volume for destination files.
    """
    for key, file in source.walk(src=path):
        target.store(file=file, dst=key)
