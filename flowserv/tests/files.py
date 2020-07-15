# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""helper classes and methods for unit tests that perform I/O operations."""

import os

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

    def save(self, filename):
        """Write simple text to given file."""
        # Ensure that the directory for the file exists.
        util.create_dir(os.path.dirname(filename))
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
        return filename
