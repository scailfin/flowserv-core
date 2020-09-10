# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""helper classes and methods for unit tests that perform I/O operations."""

import json
import os

from io import BytesIO
from typing import Dict, IO, List

import flowserv.util as util


class FakeStream(object):
    """Fake stream object to test upload from stream. Needs to implement the
    save(filename) method.
    """
    def __init__(self, data=None, format=None):
        """Set the file data object that will be written when the save method
        is called.

        Parameters
        ----------
        data: dict, optional
            File data object

        Returns
        -------
        string
        """
        self.data = data if data is not None else dict()
        self.format = format if format is not None else util.FORMAT_JSON

    def save(self, buf=None):
        """Write simple text to given bytes buffer."""
        buf = BytesIO() if buf is None else buf
        buf.seek(0)
        if self.format == util.FORMAT_JSON:
            buf.write(str.encode(json.dumps(self.data)))
        else:
            for line in self.data:
                buf.write(str.encode('{}\n'.format(line)))
        return buf

    def write(self, filename):
        """Write data to given file."""
        # Ensure that the directory for the file exists.
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        if self.format == util.FORMAT_JSON:
            util.write_object(
                filename=filename,
                obj=self.data,
                format=util.FORMAT_JSON
            )
        else:
            with open(filename, 'w') as f:
                for line in self.data:
                    f.write('{}\n'.format(line))


# -- S3 Buckets ---------------------------------------------------------------

class MemBucket(object):
    """Implementation of relevant methods for S3 buckets that are used by the
    BucketStore for test purposes. Maintains all objects in a dictionary.
    """
    def __init__(self):
        """Initialize the internal object dictionary."""
        self._objects = dict()

    @property
    def objects(self):
        """Simulate .objects call by returning a reference to self."""
        return self

    def delete_objects(self, Delete: Dict):
        """Delete objects in a dictionary with single key 'Objects' that points
        to a list of dictionaries with single element 'Key' referencing the
        object that is being deleted.
        """
        for obj in Delete.get('Objects'):
            del self._objects[obj.get('Key')]

    def download_fileobj(self, key: str, data: IO):
        """Copy the buffer for the identified object into the given data
        buffer.
        """
        try:
            buf = self._objects[key]
        except KeyError as ex:
            import botocore.exceptions
            raise botocore.exceptions.ClientError(
                operation_name='download_fileobj',
                error_response={'Error': {'Code': 404, 'Message': str(ex)}}
            )
        buf.seek(0)
        data.write(buf.read())
        data.seek(0)

    def filter(self, Prefix: str) -> List:
        """Return all objects in the bucket that have a key which matches the
        given prefix.
        """
        result = list()
        for key in self._objects:
            if key.startswith(Prefix):
                result.append(ObjectSummary(key))
        return result

    def upload_file(self, file: str, dst: str):
        """Read file and add the data buffer to the object index."""
        data = BytesIO()
        with open(file, 'rb') as f:
            data.write(f.read())
        self.upload_fileobj(file=data, dst=dst)

    def upload_fileobj(self, file: IO, dst: str):
        """Add given buffer to the object index. Uses the destination as the
        object key.
        """
        self._objects[dst] = file


class ObjectSummary(object):
    """Simple class to simulate object summaries. Only implements the .key
    property.
    """
    def __init__(self, key):
        """Initialize the object key."""
        self.key = key
