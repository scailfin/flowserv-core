# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Implementation of the file store that uses the local file system."""

import os
import shutil

from io import BytesIO
from typing import Dict, Iterable, IO, List, Optional, Tuple

from flowserv.config import FLOWSERV_BASEDIR
from flowserv.model.files.base import FileStore, IOHandle
from flowserv.model.files.bucket import Bucket

import flowserv.error as err
import flowserv.util as util


# -- File store ---------------------------------------------------------------

class FSFile(IOHandle):
    """Implementation of the file object interface for files that are stored on
    the file system.
    """
    def __init__(self, filename):
        """Initialize the file name that points to a file on disk.

        Parameters
        ----------
        filename: string
            Path to an existing file on disk.
        """
        self.filename = filename

    def open(self) -> IO:
        """Get file contents as a BytesIO buffer.

        Returns
        -------
        io.BytesIO

        Raises
        ------
        flowserv.error.UnknownFileError
        """
        if not os.path.isfile(self.filename):
            raise err.UnknownFileError(self.filename)
        return util.read_buffer(self.filename)

    def size(self) -> int:
        """Get size of the file in the number of bytes.

        Returns
        -------
        int
        """
        return os.stat(self.filename).st_size

    def store(self, filename: str):
        """Write file content to disk.

        Parameters
        ----------
        filename: string
            Name of the file to which the content is written.
        """
        copy(src=self.filename, dst=filename)


class FileSystemStore(FileStore):
    """Implementation of the abstract file store class. In this implementation
    all files are maintained on the local file system under a given base
    directory.
    """
    def __init__(self, env: Dict):
        """Initialize the base directory. The directory is created if it does
        not exist.

        Parameters
        ----------
        env: dict
            Configuration object that provides access to configuration
            parameters in the environment.
        """
        self.basedir = env.get(FLOWSERV_BASEDIR)
        if self.basedir is None:
            raise err.MissingConfigurationError('API base directory')

    def copy_folder(self, key: str, dst: str):
        """Copy all files in the folder with the given key to a target folder.

        Creates the destination folder if it does not exist.

        Parameters
        ----------
        key: string
            Unique folder key.
        dst: string
            Path on the file system to the target folder.
        """
        copy_folder(src=filepath(self.basedir, key), dst=filepath(dst))

    def delete_file(self, key: str):
        """Delete the file with the given key.

        Parameters
        ----------
        key: string
            Unique file key.
        """
        filename = filepath(self.basedir, key)
        # Only attempt to delete the file if it exists.
        if os.path.exists(filename):
            os.remove(filename)

    def delete_folder(self, key: str):
        """Delete the folder with the given key.

        Parameters
        ----------
        key: string
            Unique file key.
        """
        filename = filepath(self.basedir, key)
        # Only attempt to delete the folder if it exists.
        if os.path.exists(filename):
            shutil.rmtree(filename)

    def load_file(self, key: str) -> FSFile:
        """Get a file object for the given key. Returns the path to the file on
        the local file system.

        Parameters
        ----------
        key: string
            Unique file key.

        Returns
        -------
        flowserv.model.files.fs.FSFile
        """
        return FSFile(filepath(self.basedir, key))

    def store_files(self, files: List[Tuple[IOHandle, str]], dst: str):
        """Store a given list of file objects in the file store. The file
        destination key is a relative path name. This is used as the base path
        for all files. The file list contains tuples of file object and target
        path. The target is relative to the base destination path.

        Parameters
        ----------
        file: flowserv.model.files.base.IOHandle
            The input file object.
        dst: string
            Relative target path for the stored file.
        """
        # Ensure that the target directory exists.
        target = filepath(self.basedir, dst)
        os.makedirs(target, exist_ok=True)
        # Use the file object's store method to store the file at the target
        # destination.
        for file, filename in files:
            file.store(filepath(target, filename))


# -- Bucket -------------------------------------------------------------------

class DiskBucket(Bucket):
    """Implementation of the :class:`flowserv.model.files.bucket.Bucket` interface
    for test purposes. The test bucket maintains all files in a given folder on
    the local file system.
    """
    def __init__(self, basedir: str):
        """Initialize the storage directory.

        Parameters
        ----------
        basedir: str
            Path to a directory on the local file system.
        """
        self.basedir = basedir

    def delete(self, keys: Iterable[str]):
        """Delete files that are identified by their relative path in the given
        list of object keys.

        Parameters
        ----------
        keys: iterable of string
            Unique identifier for objects that are being deleted.
        """
        for obj in keys:
            filename = filepath(self.basedir, obj)
            if os.path.exists(filename):
                os.remove(filename)

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
        data = BytesIO()
        filename = filepath(self.basedir, key)
        if os.path.isfile(filename):
            with open(filename, 'rb') as f:
                data.write(f.read())
        else:
            raise err.UnknownFileError(key)
        data.seek(0)
        return data

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
        return [key for key in parse_dir(self.basedir) if key.startswith(filter)]

    def upload(self, file: IO, key: str):
        """Store the given IO object on the file system.

        Parameters
        ----------
        file: flowserv.model.files.base.IOHandle
            Handle for uploaded file.
        key: string
            Unique object identifier.
        """
        filename = filepath(self.basedir, key)
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'wb') as f:
            f.write(file.open().read())


# -- Helper Methods -----------------------------------------------------------

def copy(src: str, dst: str):
    """Copy a file or folder from a given source to a given destination. This
    function accounts for the case where we copy an existing folder.

    Parameters
    ----------
    src: string
        Path to the source folder
    dst: string
        Path to the target folder.
    """
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    if os.path.isdir(src):
        if os.path.exists(dst):
            for file in os.listdir(src):
                copy(os.path.join(src, file), os.path.join(dst, file))
        else:
            shutil.copytree(src=src, dst=dst)
    else:
        shutil.copyfile(src=src, dst=dst)


def copy_folder(src: str, dst: str):
    """Copy all files in a source folder to a target folder.

    Creates the destination folder if it does not exist.

    Parameters
    ----------
    src: string
        Path to the source folder.
    dst: string
        Path on the file system to the target folder.
    """
    # Create target directory if it does not exist.
    os.makedirs(dst, exist_ok=True)
    # Copy all files and folders from the source folder to the target
    # folder.
    for filename in os.listdir(src):
        source = os.path.join(src, filename)
        target = os.path.join(dst, filename)
        if os.path.isdir(source):
            shutil.copytree(src=source, dst=target)
        else:
            shutil.copy(src=source, dst=target)


def filepath(*args) -> str:
    """Create a file path for a given list of path components.

    The path separator that is used in the individual components is '/'. If
    the system-specific path separator is not '/', all occurrences of '/' in
    the path components are replaced with the system-specific ones.

    Parameters
    ----------
    args: list
        List of path components.

    Returns
    -------
    str
    """

    def stdsep(value: str) -> str:
        if os.sep != '/':
            value = value.replace('/', os.sep)
        return value

    return os.sep.join([stdsep(v) for v in args])


def parse_dir(
    dirname: str, prefix: Optional[str] = '', result: Optional[List[str]] = None
) -> List[str]:
    """Parse a given directory recursively to create al ist of all files.

    Each file in the result is represented by its relative path that starts at
    the base directory that is given when first invoking the function.

    Parameters
    ----------
    dirname: string
        Path to the directory whose contents are being added to the result.
    prefix: string
        Relative path to current directory from the base directory that was
        given when the function was initially invoked.
    result: list of string, default=None
        List of files from the directories that have already been parsed.

    Returns
    -------
    list of string
    """
    result = result if result is not None else list()
    for filename in os.listdir(dirname):
        f = os.path.join(dirname, filename)
        if os.path.isdir(f):
            parse_dir(
                dirname=f,
                prefix=os.path.join(prefix, filename),
                result=result
            )
        else:
            result.append(os.path.join(prefix, filename))
    return result


def walk(
    files: List[Tuple[str, str]], result: Optional[Tuple[FSFile, str]] = None
) -> List[Tuple[FSFile, str]]:
    """Recursively add all files in a given (source, target) list folder to a
    file upload list.

    Returns a list of (FSObject, relative targetpath) pairs.

    Parameters
    ----------
    files: list of (string, string)
        Pairs of absolute source file path and relative target file path for
        files and folders that are included in the returned list.
    reult: list of (flowserv.model.files.fs.FSFile, string)
        Result list that is appended to while the file system tree is
        traversed recursively for each element in the input file list.

    Returns
    -------
    list of (flowserv.model.files.fs.FSFile, string)
    """
    result = list() if result is None else result
    for source, target in files:
        if os.path.isdir(source):
            walkdir(src=source, dst=target, files=result)
        else:
            result.append((FSFile(source), target))
    return result


def walkdir(src: str, dst: str, files: List[Tuple[FSFile, str]]):
    """Recursively add all files in a given source folder to a file upload list.
    The elements in the list are tuples of file object and relative target
    path.

    Parameters
    ----------
    str: stirng
        Path to folder of the local file system.
    dst: string
        Relative destination path for all files in the folder.
    files: list of (flowserv.model.files.fs.FSFile, string)
        Pairs of file objects and their relative target path for upload to a
        file store.
    """
    for filename in os.listdir(src):
        file = os.path.join(src, filename)
        target = filename if dst is None else os.path.join(dst, filename)
        if os.path.isdir(file):
            walkdir(src=file, dst=target, files=files)
        else:
            files.append((FSFile(file), target))
