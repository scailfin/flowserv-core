# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""helper classes and methods for unit tests that perform I/O operations."""

import robcore.core.util as util


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
        self.data = data if not data is None else dict()

    def save(self, filename):
        """Write simple text to given file."""
        util.write_object(filename=filename, obj=self.data)
