# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""helper classes and methods for unit tests that perform I/O operations."""

import json
import os

from io import BytesIO
from typing import Dict, IO, Iterable, List, Optional, Union

from flowserv.config import FLOWSERV_BASEDIR
from flowserv.model.files.base import IOHandle, IOBuffer
from flowserv.model.files.bucket import Bucket

import flowserv.error as err
import flowserv.util as util


class FileStorage(object):
    """Fake stream object that simulates a werkzeug.FileStorage object to test
    the FlaskFile object. Wraps araond a given file object.
    """
    def __init__(self, file: IOHandle):
        """Initialize the wrapped file object.

        Parameters
        ----------
        file: flowserv.model.files.IOHandle
            File data object
        """
        self.file = file

    @property
    def content_length(self) -> int:
        """Get the size of the wrapped file."""
        return self.file.size()

    def save(self, filename: str):
        """Write file to disk or to a given IO buffer."""
        self.file.store(filename)


# -- Buckets ------------------------------------------------------------------

class DiskBucket(Bucket):
    """Implementation of the :class:`flowserv.model.files.bucketBucket` interface
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
            filename = os.path.join(self.basedir, obj)
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
        filename = os.path.join(self.basedir, key)
        if os.path.isfile(filename):
            with open(filename, 'rb') as f:
                data.write(f.read())
        else:
            raise err.UnknownFileError(key)
        data.seek(0)
        return data

    def query(self, prefix: str) -> List[str]:
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
        return [key for key in parse_dir(self.basedir) if key.startswith(prefix)]

    def upload(self, file: IO, key: str):
        """Store the given IO object on the file system.

        Parameters
        ----------
        file: flowserv.model.files.base.IOHandle
            Handle for uploaded file.
        key: string
            Unique object identifier.
        """
        filename = os.path.join(self.basedir, key)
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'wb') as f:
            f.write(file.open().read())


def DiskStore(env: Dict):
    """Create an instance of the bucket store with a disk bucket."""
    from flowserv.model.files.bucket import BucketStore
    return BucketStore(bucket=DiskBucket(basedir=env.get(FLOWSERV_BASEDIR)))


def parse_dir(dirname: str, prefix: Optional[str] = '', result: Optional[List] = None):
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


# -- Helper Functions ---------------------------------------------------------


def io_file(data: Union[List, Dict], format: Optional[str] = None) -> IOBuffer:
    """Write simple text to given bytes buffer."""
    buf = BytesIO()
    buf.seek(0)
    if format is None or format == util.FORMAT_JSON:
        buf.write(str.encode(json.dumps(data)))
    else:
        for line in data:
            buf.write(str.encode('{}\n'.format(line)))
    return IOBuffer(buf)
