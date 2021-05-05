# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper classes and methods for unit tests that perform I/O operations."""

import json

from io import BytesIO
from typing import Dict, List, Optional, Union

from flowserv.volume.base import IOHandle, IOBuffer

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


def MemStore(env: Dict):
    """Create an instance of the buckect store with a memory bucket."""
    from flowserv.model.files.bucket import BucketStore
    from flowserv.model.files.mem import MemBucket
    return BucketStore(bucket=MemBucket())


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
