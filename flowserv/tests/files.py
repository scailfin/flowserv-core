# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper classes and methods for unit tests that perform I/O operations."""

from flowserv.volume.base import IOHandle


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
