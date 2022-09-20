# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Implementation of the :class:`flowserv.volume.base.GCVolume` for the
use of AWS S3 buckets.

When using the S3Bucket the AWS credentials have to be configured. See the
documentation for more details:
https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html
"""

from __future__ import annotations
from io import BytesIO
from typing import Dict, IO, Iterable, List, Optional, Tuple, TypeVar

from flowserv.volume.base import IOHandle, StorageVolume

import flowserv.error as err
import flowserv.util as util


"""Type alias for S3 bucket objects."""
S3Bucket = TypeVar('S3Bucket')


"""Type identifier for storage volume serializations."""
S3_STORE = 's3'


# -- File handle --------------------------------------------------------------

class S3File(IOHandle):
    """Implementation of the file object interface for files that are stored on
    S3 object bucktes.
    """
    def __init__(self, bucket: S3Bucket, key: str):
        """Initialize the S3 bucket and file key.

        Parameters
        ----------
        bucket: boto3.resources.base.ServiceResource
            S3 bucket resource.
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
        import botocore
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


# -- Storage volume -----------------------------------------------------------

class S3Volume(StorageVolume):
    """Implementation of the bucket interface for AWS S3 buckets."""
    def __init__(
        self, bucket_id: str, prefix: Optional[str] = None,
        identifier: Optional[str] = None
    ):
        """Initialize the storage bucket.

        Parameters
        ----------
        bucket_id: string
            Unique bucket identifier.
        prefix: string, default=None
            Key-prefix for all files. Only set if the store represents a sub-
            folder store for the bucket.
        identifier: string, default=None
            Unique volume identifier.
        """
        super(S3Volume, self).__init__(identifier=identifier)
        self.bucket_id = bucket_id
        self.prefix = prefix
        import boto3
        self.bucket = boto3.resource('s3').Bucket(self.bucket_id)

    def close(self):
        """The AWS S3 bucket resource does not need to be closed."""
        pass

    def describe(self) -> str:
        """Get short descriptive string about the storage volume for display
        purposes.

        Returns
        -------
        str
        """
        return 'AWS S3 storage bucket {}'.format(self.bucket_id)

    def delete(self, key: str):
        """Delete file or folder with the given key.

        Parameters
        ----------
        key: str
            Path to a file object in the storage volume.
        """
        keys = self.query(filter=util.join(self.prefix, key))
        self.bucket.delete_objects(Delete={'Objects': [{'Key': k} for k in keys]})

    def erase(self):
        """Erase the storage volume base directory and all its contents."""
        self.delete(key=None)

    @staticmethod
    def from_dict(doc) -> S3Volume:
        """Get S3 bucket storage volume instance from dictionary serialization.

        Parameters
        ----------
        doc: dict
            Dictionary serialization as returned by the ``to_dict()`` method.

        Returns
        -------
        flowserv.volume.s3.S3Volume
        """
        args = util.to_dict(doc.get('args', []))
        return S3Volume(
            identifier=doc.get('id'),
            bucket_id=args.get('bucket'),
            prefix=args.get('prefix')
        )

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
        return S3Volume(
            bucket_id=self.bucket_id,
            prefix=util.join(self.prefix, key),
            identifier=identifier
        )

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
        return S3File(key=util.join(self.prefix, key), bucket=self.bucket)

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
        return {obj.key for obj in self.bucket.objects.filter(Prefix=filter)}

    def mkdir(self, path: str):
        """Create the directory with the given (relative) path and all of its
        parent directories.

        For bucket stores no directories need to be created prior to accessing
        them.

        Parameters
        ----------
        path: string
            Relative path to a directory in the storage volume.
        """
        pass

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
        self.bucket.upload_fileobj(file.open(), util.join(self.prefix, dst))

    def to_dict(self) -> Dict:
        """Get dictionary serialization for the storage volume.

        The returned serialization can be used by the volume factory to generate
        a new instance of this volume store.

        Returns
        -------
        dict
        """
        return S3Bucket(
            identifier=self.identifier,
            bucket=self.bucket_id,
            prefix=self.prefix
        )

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
        # Ensure that the key key ends with a path separator if the key is not
        # empty.
        if src and src[-1] != '/':
            prefix = '{}/'.format(src)
        elif not src:
            prefix = ''
        else:
            prefix = src
        files = self.query(filter=util.join(self.prefix, prefix))
        return [(key, S3File(bucket=self.bucket, key=key)) for key in files]


# -- Helper functions --------------------------------------------------------

def S3Bucket(bucket: str, prefix: Optional[str] = None, identifier: Optional[str] = None) -> Dict:
    """Get configuration object for AWS S3 Storage Volume.

    Parameters
    ----------
    bucket: string
        AWS S3 bucket identifier.
    prefix: string, default=None
        Key-prefix for all files.
    identifier: string, default=None
        Optional storage volume identifier.

    Returns
    -------
    dict
    """
    return {
        'type': S3_STORE,
        'id': identifier,
        'args': [
            util.to_kvp(key='bucket', value=bucket),
            util.to_kvp(key='prefix', value=prefix)
        ]
    }
