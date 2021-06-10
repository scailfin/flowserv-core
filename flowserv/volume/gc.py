# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Implementation of the :class:`flowserv.volume.base.GCVolume` for the
use of Google Cloud File Store buckets.

For testing the GCBucket the Google Cloud credentials have to be configured.
Set up authentication by creating a service account and setting the environment
variable *GOOGLE_APPLICATION_CREDENTIALS*. See the documentation for more details:
https://cloud.google.com/storage/docs/reference/libraries#setting_up_authentication
"""

from __future__ import annotations
from io import BytesIO
from typing import Dict, IO, Iterable, List, Optional, Tuple, TypeVar

from flowserv.volume.base import IOHandle, StorageVolume

import flowserv.error as err
import flowserv.util as util


"""Type identifier for storage volume serializations."""
GC_STORE = 'gc'


"""Type alias for Google Cloud storage bucket objects."""
GCClient = TypeVar('GCClient')


# -- File handle --------------------------------------------------------------

class GCFile(IOHandle):
    """Implementation of the file object interface for files that are stored on
    Google Cloud Storage buckets.
    """
    def __init__(self, client: GCClient, bucket_name: str, key: str):
        """Initialize the S3 bucket and file key.

        Parameters
        ----------
        client: google.cloud.storage.Client
            Google Cloud storage client.
        bucket_name: string
            Bucket identifier.
        key: string
            Unique file key.
        """
        self.client = client
        self.bucket_name = bucket_name
        self.key = key

    def open(self) -> IO:
        """Get file contents as a BytesIO buffer.

        Returns
        -------
        io.BytesIO
        """
        blob = self.client.bucket(self.bucket_name).blob(self.key)
        # Load object into a new bytes buffer.
        from google.cloud import exceptions
        try:
            data = BytesIO(blob.download_as_bytes())
        except exceptions.NotFound:
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

class GCVolume(StorageVolume):
    """Implementation of the storage volume class for Google Cloud File Store
    buckets.
    """
    def __init__(
        self, bucket_name: str, prefix: Optional[str] = None,
        identifier: Optional[str] = None
    ):
        """Initialize the storage bucket from the environment settings.

        Expects the bucket name in the environment variable FLOWSERV_BUCKET.

        Parameters
        ----------
        bucket_name: string
            Unique bucket identifier.
        prefix: string, default=None
            Key-prefix for all files. Only set if the store represents a sub-
            folder store for the bucket.
        identifier: string, default=None
            Unique volume identifier.
        """
        super(GCVolume, self).__init__(identifier=identifier)
        self.bucket_name = bucket_name
        self.prefix = prefix
        # Instantiates a client. Use helper method to better support mocking
        # for unit tests.
        self.client = get_google_client()
        # Create the bucket if it does not exist.
        for bucket in self.client.list_buckets():
            if bucket.name == self.bucket_name:
                return
        self.client.create_bucket(self.bucket_name)

    def close(self):
        """The Google Cloud client resource does not need to be closed."""
        pass

    def delete(self, key: str):
        """Delete file or folder with the given key.

        Parameters
        ----------
        key: str
            Path to a file object in the storage volume.
        """
        self.delete_objects(keys=self.query(filter=util.join(self.prefix, key)))

    def delete_objects(self, keys: Iterable[str]):
        """Delete objects with the given identifier.

        Parameters
        ----------
        keys: iterable of string
            Unique identifier for objects that are being deleted.
        """
        from google.cloud import exceptions
        try:
            self.client.bucket(self.bucket_name).delete_blobs(keys)
        except exceptions.NotFound:
            pass

    def describe(self) -> str:
        """Get short descriptive string about the storage volume for display
        purposes.

        Returns
        -------
        str
        """
        return 'Google Cloud Storage bucket {}'.format(self.bucket_name)

    @staticmethod
    def from_dict(doc) -> GCVolume:
        """Get Google Cloud storage volume instance from dictionary serialization.

        Parameters
        ----------
        doc: dict
            Dictionary serialization as returned by the ``to_dict()`` method.

        Returns
        -------
        flowserv.volume.gc.GCVolume
        """
        args = util.to_dict(doc.get('args', []))
        return GCVolume(
            identifier=doc.get('id'),
            bucket_name=args.get('bucket'),
            prefix=args.get('prefix')
        )

    def erase(self):
        """Erase the storage volume base directory and all its contents."""
        self.delete(key=None)

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
        return GCVolume(
            bucket_name=self.bucket_name,
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
        return GCFile(
            key=util.join(self.prefix, key),
            client=self.client, bucket_name=self.bucket_name
        )

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
        blobs = self.client.list_blobs(self.bucket_name, prefix=filter)
        return {obj.name for obj in blobs}

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
        blob = self.client.bucket(self.bucket_name).blob(util.join(self.prefix, dst))
        blob.upload_from_file(file.open())

    def to_dict(self) -> Dict:
        """Get dictionary serialization for the storage volume.

        The returned serialization can be used by the volume factory to generate
        a new instance of this volume store.

        Returns
        -------
        dict
        """
        return GCBucket(
            identifier=self.identifier,
            bucket=self.bucket_name,
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
        return self.query(filter=util.join(self.prefix, prefix))


# -- Helper Methods -----------------------------------------------------------

def GCBucket(bucket: str, prefix: Optional[str] = None, identifier: Optional[str] = None) -> Dict:
    """Get configuration object for Google Cloud storage volume.

    Parameters
    ----------
    bucket: string
        Google Cloud Storage bucket identifier.
    prefix: string, default=None
        Key-prefix for all files.
    identifier: string, default=None
        Optional storage volume identifier.

    Returns
    -------
    dict
    """
    return {
        'type': GC_STORE,
        'id': identifier,
        'args': [
            util.to_kvp(key='bucket', value=bucket),
            util.to_kvp(key='prefix', value=prefix)
        ]
    }


def get_google_client():  # pragma: no cover
    """Helper method to get instance of the Google Cloud Storage client. This
    method was added to support mocking for unit tests.
    """
    from google.cloud import storage
    return storage.Client()
