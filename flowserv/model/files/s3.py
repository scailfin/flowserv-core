# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Implementation of the file store for backends that use S3 buckets (or S3
bucket-like) objects.
"""

import botocore
import os

from io import BytesIO
from typing import Dict, IO, List, Set, Tuple, TypeVar

from flowserv.config import FLOWSERV_BASEDIR, FLOWSERV_S3BUCKET
from flowserv.model.files.base import FileStore, IOHandle

import flowserv.error as err


# Type variable for S3 bucket objects.
B = TypeVar('B')


class BucketFile(IOHandle):
    """Implementation of the file object interface for files that are stored on
    the file system.
    """
    def __init__(self, bucket: B, key: str):
        """Initialize the S3 bucket and file key..

        Parameters
        ----------
        bucket: S3.Bucket
            Object that implements the delete, download, and upload methods of
            the S3.Bucket interface.
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
        # Load object into a new bytes buffer.
        data = BytesIO()
        try:
            self.bucket.download_fileobj(self.key, data)
        except botocore.exceptions.ClientError:
            raise err.UnknownFileError(self.key)
        # Ensure to reset the read pointer of the buffer before returning it.
        data.seek(0)
        return data

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
    def __init__(self, env: Dict, bucket: B = None):
        """Initialize the storage bucket.

        Parameters
        ----------
        env: dict
            Configuration object that provides access to configuration
            parameters in the environment.
        bucket: S3.Bucket
            Object that implements the delete, download, and upload methods of
            the S3.Bucket interface.
        """
        if bucket is None:
            bucket_id = env.get(FLOWSERV_S3BUCKET)
            if bucket_id is None:
                from flowserv.tests.files import DiskBucket
                bucket = DiskBucket(basedir=env.get(FLOWSERV_BASEDIR))
            else:  # pragma: no cover
                import boto3
                bucket = boto3.resource('s3').Bucket(bucket_id)
        self.bucket = bucket

    def __repr__(self):
        """Get object representation ."""
        return "<BucketStore bucket={} />".format(self.bucket)

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
        objects = [{'Key': key}]
        self.bucket.delete_objects(Delete={'Objects': objects})

    def delete_folder(self, key: str):
        """Delete all files in the folder with the given key.

        Parameters
        ----------
        key: string
            Unique folder key.
        """
        # Collect the keys for all objects in the folder.
        keys = folder(key=key, bucket=self.bucket)
        # Only class the delete_objects method if the list of matched objects
        # is not empty.
        if len(keys) > 0:
            objects = [{'Key': k} for k in keys]
            self.bucket.delete_objects(Delete={'Objects': objects})

    def load_file(self, key: str) -> BucketFile:
        """Get a file object for the given key. Returns a buffer with the file
        content.

        Parameters
        ----------
        key: string
            Unique file key.

        Returns
        -------
        flowserv.model.files.s3.BucketFile

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

        Paramaters
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
            self.bucket.upload_fileobj(file.open(), key)


# -- Helper Methods -----------------------------------------------------------

def downloads(key: str, bucket: B) -> List[Tuple[str, str]]:
    """Create a list of objects that need to be downloaded based on the given
    source key. Returns a list of (key, path) where key is the key for the
    downloaded folder and path is the relative target path.

    Parameters
    ----------
    key: string
        unique folder key.
    bucket: S3.bucket
        S3 bucket object.

    Returns
    -------
    list
    """
    result = list()
    # Ensure that if the key ends with a '/'.
    key = key if key[-1] == '/' else '{}/'.format(key)
    for filekey in folder(key=key, bucket=bucket):
        # Remove the src key prefix from the file key to generate a target
        # name (i.e., relative file path) for the downloaded file.
        result.append((filekey, filekey[len(key):]))
    return result


def folder(key: str, bucket: B) -> Set[str]:
    """Find all keys in a bucket that belong to a given folder. The query key
    references a base folder of objects.

    Parameters
    ----------
    key: string
        unique folder key.
    bucket: S3.bucket
        S3 bucket object.

    Returns
    -------
    set
    """
    keyset = set()
    # Prefix for objects if the query references a directory.
    prefix = key if key[-1] == '/' else '{}/'.format(key)
    for obj in bucket.objects.filter(Prefix=prefix):
        keyset.add(obj.key)
    return keyset
