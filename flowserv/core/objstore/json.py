# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Template stores that maintain templates as Json objects."""

import os

from flowserv.core.objstore.base import ObjectStore

import flowserv.core.error as err
import flowserv.core.util as util


class JsonFileStore(ObjectStore):
    """The Json file store maintains workflow template specifications as
    separate files on disk. Files are stored under a given base directory. The
    file format is Json.

    For each template the store will either create a file in the base directory
    using the template identifier as name or create a file with a given default
    name in a sub-directory named after the template identifier. This behavior
    is controlled by the default_filename attribute. If the attribute is None
    the former case is True, otherwise the latter case.
    """
    def __init__(self, basedir, default_filename=None):
        """Initialize the base directory and the default file name. Templates
        are stored in files under the base directory. Depending on whether the
        default file name is given, templates are either stored as files named
        using the template identifier (default_filename is None) or in a sub-
        directory named after the template identifier (default_filename is not
        None).
        """
        # Set the directory and ensure that it exists
        self.basedir = util.create_dir(basedir)
        self.default_filename = default_filename

    def get_filename(self, identifier):
        """The name for a template file depends on the value of the default
        file name attribute.

        Parameters
        ----------
        identifier: string
            Unique template identifier

        Returns
        -------
        string
        """
        if self.default_filename is None:
            # The file is in the base directory named after the template
            # identifier
            return os.path.join(self.basedir, '{}.json'.format(identifier))
        else:
            # The file is in a sub-directory named after the identifier
            sub_dir = util.create_dir(os.path.join(self.basedir, identifier))
            return os.path.join(sub_dir, self.default_filename)

    def read(self, identifier):
        """Read object with the given identifier from the store. Raises an error
        if the identifier is unknown.

        Parameters
        ----------
        identifier: string
            Unique object identifier.

        Returns
        -------
        dict

        Raises
        ------
        flowserv.core.error.UnknownObjectError
        """
        filename = self.get_filename(identifier)
        # Raise an error if the file does not exist
        if not os.path.isfile(filename):
            raise err.UnknownObjectError(obj_id=identifier)
        # Read file from disk and return template instance. The base directory
        # for the template instance is the folder that contains the
        return util.read_object(filename, format=util.FORMAT_JSON)

    def write(self, identifier, obj):
        """Write the given object to the store. Replaces an existing object
        with the same identifier if it exists.

        Parameters
        ----------
        identifier: string
            Unique object identifier
        obj: dict
            Object instance
        """
        filename = self.get_filename(identifier)
        util.write_object(
            obj=obj,
            filename=filename,
            format=util.FORMAT_JSON
        )
