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

from io import BytesIO
from typing import Dict, IO, Iterable

from flowserv.config import FLOWSERV_BUCKET
from flowserv.model.files.base import IOHandle
from flowserv.model.files.bucket import Bucket

import flowserv.error as err


class S3Bucket(Bucket):
    """Implementation of the bucket interface for AWS S3 buckets."""
    def __init__(self, env: Dict):
        """Initialize the storage bucket.

        Parameters
        ----------
        env: dict
            Configuration object that provides access to configuration
            parameters in the environment.
        """
        bucket_id = env.get(FLOWSERV_BUCKET)
        if bucket_id is None:
            raise err.MissingConfigurationError('bucket identifier')
        import boto3
        self.bucket = boto3.resource('s3').Bucket(bucket_id)

    def delete(self, keys: Iterable[str]):
        """Delete objects with the given identifier.

        Parameters
        ----------
        keys: iterable of string
            Unique identifier for objects that are being deleted.
        """
        self.bucket.delete_objects(Delete={'Objects': [{'Key': k} for k in keys]})

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
        # Load object into a new bytes buffer.
        data = BytesIO()
        try:
            self.bucket.download_fileobj(key, data)
        except botocore.exceptions.ClientError:
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
        return {obj.key for obj in self.bucket.objects.filter(Prefix=filter)}

    def upload(self, file: IOHandle, key: str):
        """Upload the file object and store it under the given key.

        Parameters
        ----------
        file: flowserv.model.files.base.IOHandle
            Handle for uploaded file.
        key: string
            Unique object identifier.
        """
        self.bucket.upload_fileobj(file.open(), key)
