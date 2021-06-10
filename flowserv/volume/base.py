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
from typing import Dict, IO, List, Optional, Tuple, Union

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

    def copy(
        self, src: Union[str, List[str]], store: StorageVolume, dst: Optional[str] = None,
        verbose: Optional[bool] = False
    ) -> List[str]:
        """Copy the file or folder at the source path of this storage volume to
        the given storage volume.

        The source path is relative to the base directory for the workflow run.

        Returns the list of files that were copied.

        Parameters
        ----------
        src: string or list of string
            Relative source path(s) for downloaded files and directories.
        store: flowserv.volume.base.StorageValue
            Storage volume for destination files.
        dst: string, default=None
            Destination folder for downloaded files.
        verbose: bool, default=False
            Print information about source and target volume and the files that
            are being copied.

        Returns
        -------
        list of string
        """
        return copy_files(src=src, source=self, dst=dst, target=store, verbose=verbose)

    @abstractmethod
    def delete(self, key: str) -> int:
        """Delete file or folder with the given key.

        Parameters
        ----------
        key: str
            Path to a file object in the storage volume.
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def describe(self) -> str:
        """Get short descriptive string about the storage volume for display
        purposes.

        Returns
        -------
        str
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def erase(self):
        """Erase the storage volume base directory and all its contents."""
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def get_store_for_folder(self, key: str, identifier: Optional[str] = None) -> StorageVolume:
        """Get storage volume for a sob-folder of the given volume.

        Parameters
        ----------
        key: string
            Relative path to sub-folder. The concatenation of the base folder
            for this storage volume and the given key will form te new base
            folder for the returned storage volume.
        identifier: string, default=None
            Unique volume identifier.

        Returns
        -------
        flowserv.volume.base.StorageVolume
        """
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
    def mkdir(self, path: str):
        """Create the directory with the given (relative) path and all of its
        parent directories.

        Does not raise an error if the directory exists.

        Parameters
        ----------
        path: string
            Relative path to a directory in the storage volume.
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

    @abstractmethod
    def to_dict(self) -> Dict:
        """Get dictionary serialization for the storage volume.

        The returned serialization can be used by the volume factory to generate
        a new instance of this volume store.

        Returns
        -------
        dict
        """
        raise NotImplementedError()  # pragma: no cover

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

def copy_files(
    src: Union[str, List[str]], source: StorageVolume, dst: str, target: StorageVolume,
    verbose: Optional[bool] = False
) -> List[str]:
    """Copy files and folders at the source path (path) of a given source
    storage volume to the destination path (path) of a target storage volume.

    Returns the list of files that were copied.

    Parameters
    ----------
    src: str or list of string
        Path specifying the source file(s) or folder(s).
    source: flowserv.volume.base.StorageValue
        Storage volume for source files.
    dst: string
        Destination path for copied files.
    target: flowserv.volume.base.StorageValue
        Storage volume for destination files.
    verbose: bool, default=False
        Print information about source and target volume and the files that are
        being copied.

    Returns
    -------
    list of string
    """
    if verbose:
        print('Copy files from {} to {}'.format(source.describe(), target.describe()))
    files = list()
    for path in src if isinstance(src, list) else [src]:
        # Get list of source files to copy. If a single element is returned
        # with a key that equals the 'path' then we are copying a file. In this
        # case the source path is copied to the given dst path (or the source
        # path is dst is None). If we are opying a directory and the destination
        # path is given, we remove the 'path' from all keys.
        source_files = source.walk(src=path)
        if len(source_files) == 1 and source_files[0][0] == path:
            # We are copying a single file.
            _, file = source_files[0]
            dstpath = dst if dst is not None else path
            files.append(dstpath)
            target.store(file=file, dst=dstpath)
            if verbose:
                print('copied {} to {}'.format(path, dstpath))
        else:
            # We are copying a directory. If the destination path is given,
            # make sure to remove the 'path' from all keys.
            for key, file in source_files:
                if path:
                    prefix = path + '/'
                    key = key[len(prefix):]
                dstpath = util.join(dst, key) if dst else key
                files.append(dstpath)
                target.store(file=file, dst=dstpath)
                if verbose:
                    print('copied {} to {}'.format(key, dstpath))
    return files
