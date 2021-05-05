# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Implementation of the :class:`flowserv.model.files.bucket.Bucket` for the
use of AWS S3 buckets.

When using the S3Bucket the AWS credentials have to be configured. See the
documentation for more details:
https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html
"""

import botocore

from io import BytesIO
from typing import Dict, IO, Iterable, List, Optional, Tuple, TypeVar

from flowserv.config import FLOWSERV_BUCKET
from flowserv.volume.base import IOHandle, StorageVolume

import flowserv.error as err

S3Bucket = TypeVar('S3Bucket')


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
    def __init__(self, env: Dict, identifier: Optional[str] = None):
        """Initialize the storage bucket.

        Parameters
        ----------
        env: dict
            Configuration object that provides access to configuration
            parameters in the environment.
        identifier: string, default=None
            Unique volume identifier.
        """
        super(S3Volume, self).__init__(identifier=identifier)
        bucket_id = env.get(FLOWSERV_BUCKET)
        if bucket_id is None:
            raise err.MissingConfigurationError('bucket identifier')
        import boto3
        self.bucket = boto3.resource('s3').Bucket(bucket_id)

    def close(self):
        """The AWS S3 bucket resource does not need to be closed."""
        pass

    def erase(self):
        """Erase the storage volume base directory and all its contents."""
        keys = self.query(filter=None)
        self.bucket.delete_objects(Delete={'Objects': [{'Key': k} for k in keys]})

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
        return S3File(key=key, bucket=self.bucket)

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
        self.bucket.upload_fileobj(file.open(), dst)

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
