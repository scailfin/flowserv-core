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
from typing import Dict, IO, Iterable

from google.cloud import exceptions

from flowserv.model.files.base import IOHandle
from flowserv.model.files.bucket import Bucket

import flowserv.config as config
import flowserv.error as err


class GCBucket(Bucket):
    """Implementation of the bucket interface for Google Cloud File Store
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
        blob = self.client.bucket(self.bucket_name).blob(key)
        # Load object into a new bytes buffer.
        try:
            data = BytesIO(blob.download_as_bytes())
        except exceptions.NotFound:
            raise err.UnknownFileError(key)
        # Ensure to reset the read pointer of the buffer before returning it.
        data.seek(0)
        return data

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

    def upload(self, file: IOHandle, key: str):
        """Upload the file object and store it under the given key.

        Parameters
        ----------
        file: flowserv.model.files.base.IOHandle
            Handle for uploaded file.
        key: string
            Unique object identifier.
        """
        blob = self.client.bucket(self.bucket_name).blob(key)
        blob.upload_from_file(file.open())


# -- Helper Methods -----------------------------------------------------------

def get_google_client():  # pragma: no cover
    """Helper method to get instance of the Google Cloud Storage client. This
    method was added to support mocking for unit tests.
    """
    from google.cloud import storage
    return storage.Client()
