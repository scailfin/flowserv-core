# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Handles for file resources that are created by successful workflow runs."""

import os


class FileResource(object):
    """Handle for a file resource that is created as the result of a workflow
    run. The handle contains the reference to the persistent file that is stored
    on disk. The file should be maintained by the workflow backend in a
    persistent manner in order to be accessible as long as information about
    the workflow run is maintained by the workflow engine.
    """
    def __init__(self, identifier, filename):
        """Initialize the resource identifier and the path to the (persistently)
        created file.

        Parameters
        ----------
        identifier: string
            Resource identifier that is unique among all resources that are
            created within a single workflow run.
        filename: string
            Path to persistent file on disk.
        """
        self.identifier = identifier
        self.filename = filename

    def delete(self):
        """Delete the associated file from disk."""
        os.remove(self.filename)
