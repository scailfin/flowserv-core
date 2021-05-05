# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Implementation of the :class:`flowserv.model.files.bucket.Bucket` for the
use of Google Cloud File Store buckets.

For testing the GCBucket the Google Cloud credentials have to be configured.
Set up authentication by creating a service account and setting the environment
variable *GOOGLE_APPLICATION_CREDENTIALS*. See the documentation for more details:
https://cloud.google.com/storage/docs/reference/libraries#setting_up_authentication
"""

from io import BytesIO
from typing import Dict, IO, Iterable, List, Tuple, TypeVar

from google.cloud import exceptions

from flowserv.volume.base import IOHandle, StorageVolume

import flowserv.config as config
import flowserv.error as err


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


class GCVolume(StorageVolume):
    """Implementation of the storage volume class for Google Cloud File Store
    buckets.
    """
    def __init__(self, env: Dict):
        """Initialize the storage bucket from the environment settings.

        Expects the bucket name in the environment variable FLOWSERV_BUCKET.

        Parameters
        ----------
        env: dict
            Configuration object that provides access to configuration
            parameters in the environment.
        """
        self.bucket_name = env.get(config.FLOWSERV_BUCKET)
        if self.bucket_name is None:
            raise err.MissingConfigurationError('bucket name')
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

    def delete(self, keys: Iterable[str]):
        """Delete objects with the given identifier.

        Parameters
        ----------
        keys: iterable of string
            Unique identifier for objects that are being deleted.
        """
        try:
            self.client.bucket(self.bucket_name).delete_blobs(keys)
        except exceptions.NotFound:
            pass

    def erase(self):
        """Erase the storage volume base directory and all its contents."""
        self.delete(keys=self.query(filter=None))

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
        return GCFile(key=key, client=self.client, bucket_name=self.bucket_name)

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
        blob = self.client.bucket(self.bucket_name).blob(dst)
        blob.upload_from_file(file.open())

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
        return self.query(filter=prefix)


# -- Helper Methods -----------------------------------------------------------

def get_google_client():  # pragma: no cover
    """Helper method to get instance of the Google Cloud Storage client. This
    method was added to support mocking for unit tests.
    """
    from google.cloud import storage
    return storage.Client()
