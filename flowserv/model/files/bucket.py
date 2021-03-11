# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Implementation of the file store for backends that use storage buckets (e.g.,
clound service providers like AWS S3 or Google Cloud File Store).

Defines an interface `flowserv.model.files.bucket.Bucket` with methods to upload,
download, and delete objects. The implementation of the base bucket store
assumes that object in the storage buckets are identified by arbitrary user-defined
strings. The strings are expected to be relative path names.
"""

from abc import ABCMeta, abstractmethod
from typing import IO, Iterable, List, Set, Tuple

import os

from flowserv.model.files.base import FileStore, IOHandle


class Bucket(metaclass=ABCMeta):
    """Interface for a bucket object that allows to upload, download, and delete
    blob file objects.
    """
    @abstractmethod
    def delete(self, keys: Iterable[str]):
        """Delete objects with the given identifier.

        Parameters
        ----------
        keys: iterable of string
            Unique identifier for objects that are being deleted.
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def download(self, key: str) -> IO:
        """Get content for the object with the given key as an IO buffer.

        The buffer is expected to be readable from the beginning without the
        need to reset the read pointer.

        Parameters
        ----------
        key: string
            Unique object identifier.

        Returns
        -------
        io.BytesIO
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def query(self, filter: str) -> Iterable[str]:
        """Get identifier for objects that match a given prefix.

        Parameters
        ----------
        filter: str
            Prefix query for object identifiers.

        Returns
        -------
        iterable of string
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def upload(self, file: IOHandle, key: str):
        """Upload the file object and store it under the given key.

        Parameters
        ----------
        file: flowserv.model.files.base.IOHandle
            Handle for uploaded file.
        key: string
            Unique object identifier.
        """
        raise NotImplementedError()  # pragma: no cover


class BucketFile(IOHandle):
    """Implementation of the file object interface for files that are stored on
    the file system.
    """
    def __init__(self, bucket: Bucket, key: str):
        """Initialize the S3 bucket and file key..

        Parameters
        ----------
        bucket: flowserv.model.files.bucket.Bucket
            Object that implements the bucket interface.
        key: string
            Unique file key.
        """
        self.bucket = bucket
        self.key = key

    def open(self) -> IO:
        """Get file contents as a BytesIO buffer.

        Returns
        -------
        io.BytesIO
        """
        return self.bucket.download(self.key)

    def size(self) -> int:
        """Get size of the file in the number of bytes.

        Returns
        -------
        int
        """
        return self.open().getbuffer().nbytes

    def store(self, filename: str) -> int:
        """Write file content to disk. Returns the size of the written file.

        Parameters
        ----------
        filename: string
            Name of the file to which the content is written.

        Returns
        -------
        int
        """
        with open(filename, 'wb') as f:
            f.write(self.open().read())


class BucketStore(FileStore):
    """Implementation of the abstract file store class. In this implementation
    all files are maintained on the local file system under a given base
    directory.
    """
    def __init__(self, bucket: Bucket):
        """Initialize the storage bucket.

        Parameters
        ----------
        bucket: flowserv.model.files.bucket.Bucket
            Object that implements the bucket interface.
        """
        self.bucket = bucket

    def copy_folder(self, key: str, dst: str):
        """Copy all files in the folder with the given key to a target folder
        on the local file system. Ensures that the target folder exists.

        Parameters
        ----------
        key: string
            Unique folder key.
        dst: string
            Path on the file system to the target folder.
        """
        os.makedirs(dst, exist_ok=True)
        # Get list of all files in the folder.
        for filekey, target in downloads(key=key, bucket=self.bucket):
            outfile = os.path.join(dst, target)
            # Create parent folder for the target file exist.
            os.makedirs(os.path.dirname(outfile), exist_ok=True)
            data = self.load_file(filekey)
            with open(outfile, 'wb') as f:
                f.write(data.open().read())

    def delete_file(self, key: str):
        """Delete the file with the given key.

        Parameters
        ----------
        key: string
            Unique file key.
        """
        self.bucket.delete([key])

    def delete_folder(self, key: str):
        """Delete all files in the folder with the given key.

        Parameters
        ----------
        key: string
            Unique folder key.
        """
        # Collect the keys for all objects in the folder.
        keys = folder(key=key, bucket=self.bucket)
        # Only call the delete_objects method if the list of matched objects
        # is not empty.
        if len(keys) > 0:
            self.bucket.delete(keys)

    def load_file(self, key: str) -> BucketFile:
        """Get a file object for the given key. Returns a buffer with the file
        content.

        Parameters
        ----------
        key: string
            Unique file key.

        Returns
        -------
        flowserv.model.files.bucket.BucketFile

        Raises
        ------
        flowserv.error.UnknownFileError
        """
        return BucketFile(bucket=self.bucket, key=key)

    def store_files(self, files: List[Tuple[IOHandle, str]], dst: str):
        """Store a given list of file objects in the associated bucket. The
        file destination key is a relative path name. This is used as the base
        path for all files. The file list contains tuples of file object and
        target path. The target is relative to the base destination path.

        Parameters
        ----------
        file: flowserv.model.files.base.IOHandle
            The input file object.
        dst: string
            Relative target path for the stored file.

        Returns
        -------
        int
        """
        # Use the file object's store method to store the file at the target
        # destination.
        for file, filename in files:
            key = os.path.join(dst, filename)
            self.bucket.upload(file=file, key=key)


# -- Helper Methods -----------------------------------------------------------

def downloads(key: str, bucket: Bucket) -> List[Tuple[str, str]]:
    """Create a list of objects that need to be downloaded based on the given
    source key. Returns a list of (key, path) where key is the key for the
    downloaded folder and path is the relative target path.

    Parameters
    ----------
    key: string
        unique folder key.
    bucket: flowserv.model.files.bucket.Bucket
        Object that implements the bucket interface.

    Returns
    -------
    list
    """
    result = list()
    # Ensure that if the key ends with a path separator.
    key = key if key[-1] == os.path.sep else '{}{}'.format(key, os.path.sep)
    for filekey in folder(key=key, bucket=bucket):
        # Remove the src key prefix from the file key to generate a target
        # name (i.e., relative file path) for the downloaded file.
        result.append((filekey, filekey[len(key):]))
    return result


def folder(key: str, bucket: Bucket) -> Set[str]:
    """Find all keys in a bucket that belong to a given folder. The query key
    references a base folder of objects.

    Parameters
    ----------
    key: string
        unique folder key.
    bucket: flowserv.model.files.bucket.Bucket
        Object that implements the bucket interface.

    Returns
    -------
    set
    """
    # Prefix for objects if the query references a directory.
    prefix = key if key[-1] == os.path.sep else '{}{}'.format(key, os.path.sep)
    return bucket.query(prefix=prefix)
