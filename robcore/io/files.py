# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""The reproducible benchmark engine uses a simple file store to maintain files
that are uploaded by users as part of the inputs to workflow runs.

Uploaded files are currently stored on disk. The file store maintains uploaded
files within sub-folders of a base folder. Each file has a unique identifier
that is generated by the system. The file handle is a wrapper around the local
file to provide access to the file.
"""

import os
from datetime import datetime

import robcore.util as util


class FileDescriptor(object):
    """Descriptor for an uploaded file. Contains the file identifier, name,
    and the upload timestamp.
    """
    def __init__(self, identifier, name, created_at):
        """Initialize the object properties.

        Parameters
        ----------
        identifier: string
            Unique file identifier
        name: string
            Name of the uploaded file
        created_at: datetime.datetime
            Timestamp of file upload (in UTC timezone)
        """
        self.identifier = identifier
        self.name = name
        self.created_at = created_at

    def upload_time(self):
        """Get string representation of the upload timestamp (created_at) in
        local timezone.

        Returns
        -------
        string
        """
        return util.to_localstr(date=self.created_at)


class FileHandle(object):
    """Handle for files that are managed by the file store. Each file has a
    unique identifier and a file name. Files are maintaind in folders on the
    file system.
    """
    def __init__(self, filepath, identifier=None, file_name=None, mimetype=None):
        """Initialize the file identifier, the (full) file path, and the file
        name. The file path is mandatory.

        Parameters
        ----------
        filepath: string
            Absolute path to file on disk
        identifier: string, optional
            Unique file identifier
        file_name: string, optional
            Base name of the file
        mimetype: string, optional
            File mime-type (if known)
        """
        self.filepath = os.path.abspath(filepath)
        self.identifier = identifier if identifier is not None else util.get_unique_identifier()
        self.file_name = file_name if file_name is not None else os.path.basename(self.filepath)
        self.mimetype = mimetype

    @property
    def created_at(self):
        """Date and time when file was created (e.g., uploaded). The timestamp
        is given in the UTC time zone.

        Returns
        -------
        datetime.date
        """
        return datetime.utcfromtimestamp(os.path.getctime(self.filepath))

    def delete(self):
        """Remove the associated file on disk (if it exists)."""
        if os.path.isfile(self.filepath):
            os.remove(self.filepath)

    @property
    def last_modified(self):
        """Last modification timestamp for the file.

        Returns
        -------
        datetime
        """
        datetime.utcfromtimestamp(os.path.getmtime(self.filepath))

    @property
    def name(self):
        """Method for accessing the file name.

        Returns
        -------
        string
        """
        return self.file_name

    @property
    def size(self):
        """Get size of file in bytes.

        Returns
        -------
        int
        """
        return os.stat(self.filepath).st_size


class InputFile(FileHandle):
    """The InputFile represents the value for a template parameter of type
    'file'. This class extends the handle for an uploaded file with an optional
    target path that the user may have provided.
    """
    def __init__(self, f_handle, target_path=None):
        """Initialize the object properties.

        Parameters
        ----------
        f_handle: robcore.io.files.FileHandle
        target_path: string, optional
        """
        super(InputFile, self).__init__(
            filepath=f_handle.filepath,
            identifier=f_handle.identifier,
            file_name=f_handle.file_name
        )
        self.target_path = target_path

    def source(self):
        """Shortcut to get the source path for the file.

        Returns
        -------
        string
        """
        return self.filepath

    def target(self):
        """Shortcut to get the target path for the file.

        Returns
        -------
        string
        """
        if not self.target_path is None:
            return self.target_path
        else:
            return self.file_name
