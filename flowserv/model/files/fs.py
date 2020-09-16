# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Implementation of the file store that uses the local file system."""

import os
import shutil

from io import StringIO
from typing import IO, List, Tuple, Union

from flowserv.model.files.base import FileStore

import flowserv.config.api as config
import flowserv.config.files as fconfig
import flowserv.util as util


class FileSystemStore(FileStore):
    """Implementation of the abstract file store class. In this implementation
    all files are maintained on the local file system under a given base
    directory.
    """
    def __init__(self, basedir: str = None):
        """Initialize the base directory. The directory is created if it does
        not exist.

        Parameters
        ----------
        basedir: string, default=None
            Path to the base directory.
        """
        self.basedir = basedir if basedir is not None else config.API_BASEDIR()

    def __repr__(self):
        """Get object representation ."""
        return "<FileSystemStore dir='{}' />".format(self.basedir)

    def configuration(self) -> List[Tuple[str, str]]:
        """Get a list of tuples with the names of additional configuration
        variables and their current values.

        Returns
        -------
        list((string, string))
        """
        return [
            (fconfig.FLOWSERV_FILESTORE_CLASS, 'FileSystemStore'),
            (fconfig.FLOWSERV_FILESTORE_MODULE, 'flowserv.model.files.fs')
        ]

    def copy_files(self, src: str, files: List[Tuple[str, str]]):
        """Copy a list of files or dirctories from a given source directory.
        The list of files contains tuples of relative file source and target
        path. The source path may reference existing files or directories.

        Raises a ValueError if an attempt is made to overwrite an existing
        file.

        Parameters
        ----------
        src: string
            Path to source directory on disk.
        files: list((string, string))
            List of file source and target path. All path names are relative.

        Raises
        ------
        ValueError
        """
        filelist = list()
        for source, target in files:
            source = os.path.join(src, source)
            filelist.append((source, target))
        util.copy_files(
            files=filelist,
            target_dir=self.basedir,
            overwrite=False,
            raise_error=True
        )

    def delete_file(self, key: str):
        """Delete the file with the given key.

        Parameters
        ----------
        key: string
            Unique file key.
        """
        filename = os.path.join(self.basedir, key)
        if os.path.isfile(filename):
            os.remove(filename)
        elif os.path.isdir(filename):
            shutil.rmtree(filename)

    def download_archive(self, src: str, files: List[str]) -> IO:
        """Download all files in the given list from the specified source
        directory as a tar archive.

        Parameters
        ----------
        src: string
            Relative path to the files source directory.
        files: list(string)
            List of relative paths to files (or directories) in the specified
            source directory. Lists the files to include in the returned
            archive.

        Returns
        -------
        io.BytesIO
        """
        src = os.path.join(self.basedir, src)
        return util.archive_files([(os.path.join(src, f), f) for f in files])

    def download_files(self, files: List[Tuple[str, str]], dst: str):
        """Copy a list of files or dirctories from the file store to a given
        destination directory. The list of files contains tuples of relative
        file source and target path. The source path may reference files or
        directories.

        Parameters
        ----------
        files: list((string, string))
            List of file source and target path. All path names are relative.
        dst: string
            Path to target directory on disk.

        Raises
        ------
        ValueError
        """
        filelist = list()
        for source, target in files:
            source = os.path.join(self.basedir, source)
            filelist.append((source, target))
        util.copy_files(
            files=filelist,
            target_dir=dst,
            overwrite=False,
            raise_error=True
        )

    def load_file(self, key: str) -> str:
        """Get a file object for the given key. Returns the path to the file on
        the local file system.

        Parameters
        ----------
        key: string
            Unique file key.

        Returns
        -------
        string

        Raises
        ------
        flowserv.error.UnknownFileError
        """
        return os.path.join(self.basedir, key)

    def upload_file(self, file: Union[str, IO], dst: str) -> int:
        """Upload a given file object to the file store. The destination path
        is a relative path. The file may reference a file on the local file
        system or it is a file object (StringIO or BytesIO).

        Returns the size of the uploaded file on disk.

        Paramaters
        ----------
        file: string or io.BytesIO or io.StringIO
            The input file is either a FileObject (buffer) or a reference to a
            file on the local file system.
        dst: string
            Relative target path for the stored file.

        Returns
        -------
        int
        """
        target = os.path.join(self.basedir, dst)
        os.makedirs(os.path.dirname(target), exist_ok=True)
        # Depending on the type of the file parameter copy an input file or
        # write the contents of a file buffer object to disk.
        if isinstance(file, str):
            shutil.copy(src=file, dst=target)
        elif isinstance(file, StringIO):
            with open(target, 'w') as fd:
                file.seek(0)
                shutil.copyfileobj(file, fd)
        else:  # assumes isinstance(file, BytesIO)
            with open(target, 'wb') as fd:
                fd.write(file.getbuffer())
        # Return size of the created file.
        return os.stat(target).st_size
