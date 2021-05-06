# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Handle for file-like objects that are stored in a storage volume."""


from abc import ABCMeta, abstractmethod
from io import BytesIO
from typing import IO


# -- Wrapper for database files -----------------------------------------------

class FileHandle(IOHandle):
    """Handle for a file that is stored in the database. Extends the file object
    with the base file name and the mime type.

    The implementation is a wrapper around a file object to make the handle
    agnostic to the underlying storage mechanism.
    """
    def __init__(self, name: str, mime_type: str, fileobj: IOHandle):
        """Initialize the file object and file handle.

        Parameters
        ----------
        name: string
            File name (or relative file path)
        mime_type: string
            File content mime type.
        fileobj: flowserv.model.files.base.IOHandle
            File object providing access to the file content.
        """
        self.name = name
        self.mime_type = mime_type
        self.fileobj = fileobj

    def open(self) -> IO:
        """Get an BytesIO buffer containing the file content. If the associated
        file object is a path to a file on disk the file is being read.

        Returns
        -------
        io.BytesIO

        Raises
        ------
        flowserv.error.UnknownFileError
        """
        return self.fileobj.open()

    def size(self) -> int:
        """Get size of the file in the number of bytes.

        Returns
        -------
        int
        """
        return self.fileobj.size()

    def store(self, filename: str):
        """Write file contents to disk.

        Parameters
        ----------
        filename: string
            Name of the file to which the content is written.
        """
        self.fileobj.store(filename)


# -- Wrapper for files that are uploaded as part of a Flask request -----------

class FlaskFile(IOHandle):
    """File object implementation for files that are uploaded via Flask
    requests as werkzeug.FileStorage objects.
    """
    def __init__(self, file):
        """Initialize the reference to the uploaded file object.

        Parameters
        ----------
        file: werkzeug.FileStorage
            File object that was uploaded as part of a Flask request.
        """
        self.file = file

    def open(self) -> IO:
        """Get file contents as a BytesIO buffer.

        Returns
        -------
        io.BytesIO

        Raises
        ------
        flowserv.error.UnknownFileError
        """
        buf = BytesIO()
        self.file.save(buf)
        buf.seek(0)
        return buf

    def size(self) -> int:
        """Get size of the file in the number of bytes.

        Returns
        -------
        int
        """
        return self.file.content_length

    def store(self, filename: str):
        """Write file content to disk.

        Parameters
        ----------
        filename: string
            Name of the file to which the content is written.
        """
        self.file.save(filename)
