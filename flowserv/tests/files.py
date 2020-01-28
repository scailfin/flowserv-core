# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""helper classes and methods for unit tests that perform I/O operations."""

import flowserv.core.util as util


class FakeStream(object):
    """Fake stream object to test upload from stream. Needs to implement the
    save(filename) method.
    """
    def __init__(self, data=None):
        """Set the file data object that will be written when the save method
        is called.

        Parameters
        ----------
        data: dict, optional
            File data object
        """
        self.data = data if data is not None else dict()

    def save(self, filename):
        """Write simple text to given file."""
        util.write_object(
            filename=filename,
            obj=self.data,
            format=util.FORMAT_JSON
        )
