# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Workflow storage manager that uses a SSH client to connect to a remote
server where run files are maintained.
"""

from typing import IO, List, Optional, Tuple

import paramiko

from flowserv.volume.base import IOHandle, StorageVolume
from flowserv.util.ssh import SSHClient

import flowserv.util as util


# -- File handles -------------------------------------------------------------

class SFTPFile(IOHandle):
    """Implementation of the IO object handle interface for files that are
    stored on a remote file system.
    """
    def __init__(self, filename: str, client: SSHClient):
        """Initialize the file name that points to a remote file and the SSH
        client that is used to open the file.

        Parameters
        ----------
        filename: string
            Path to an existing file on disk.
        client: flowserv.util.ssh.SSHClient
            SSH client for accessing the remote server.
        """
        self.filename = filename
        self.client = client

    def open(self) -> IO:
        """Get file contents as a BytesIO buffer.

        Returns
        -------
        io.BytesIO

        Raises
        ------
        flowserv.error.UnknownFileError
        """
        return self.client.sftp().open(self.filename, 'rb')

    def size(self) -> int:
        """Get size of the file in the number of bytes.

        Returns
        -------
        int
        """
        sftp = self.client.sftp()
        try:
            return sftp.stat(self.filename).st_size
        finally:
            sftp.close()


class RemoteStorage(StorageVolume):
    """File storage volume that connects to a remote server via sftp."""
    def __init__(self, client: SSHClient, remotedir: str, identifier: Optional[str] = None):
        """Initialize the storage base directory on the remote server and the
        SSH connection client.

        The remote base directory is created if it does not exist. If no
        identifier is provided a unique identifier is generated by the super
        class constructor.

        Parameters
        ----------
        client: flowserv.util.ssh.SSHClient
            SSH client for accessing the remote server.
        remotedir: string
            Base directory for all run files on the remote file system.
        identifier: string, default=None
            Unique volume identifier.
        """
        super(RemoteStorage, self).__init__(identifier=identifier)
        self.client = client
        self.remotedir = remotedir
        # Create the remote directory if it does not exists.
        sftp_mkdir(client=client.sftp(), dirpath=self.remotedir)

    def close(self):
        """Close the SSH connection when workflow execution is done."""
        self.client.close()

    def delete(self, key: str):
        """Delete file or folder with the given key.

        Parameters
        ----------
        key: str
            Path to a file object in the storage volume.
        """
        sftp = self.client.sftp()
        # Get recursive list of all files in the base folder and delete them.
        dirpath = util.filepath(key=key, sep=self.client.sep)
        dirpath = self.client.sep.join([self.remotedir, dirpath]) if dirpath else self.remotedir
        files = self.client.walk(dirpath=dirpath)
        if files is None:
            filename = util.filepath(key=key, sep=self.client.sep)
            filename = self.client.sep.join([self.remotedir, filename])
            sftp.remove(filename)
        else:
            # Collect sub-directories that need to be removed separately after
            # the directories are empty.
            directories = set()
            for src in files:
                filename = util.filepath(key=src, sep=self.client.sep)
                filename = self.client.sep.join([self.remotedir, filename])
                dirname = util.dirname(src)
                if dirname:
                    directories.add(dirname)
                sftp.remove(filename)
            for dirpath in sorted(directories, reverse=True):
                dirname = util.filepath(key=dirpath, sep=self.client.sep)
                dirname = self.client.sep.join([self.remotedir, dirname]) if dirname else self.remotedir
                sftp.rmdir(dirname)

    def describe(self) -> str:
        """Get short descriptive string about the storage volume for display
        purposes.

        Returns
        -------
        str
        """
        return "remote server {}:{}".format(self.client.hostname, self.remotedir)

    def erase(self):
        """Erase the storage volume base directory and all its contents."""
        # Delete all files and folders that are reachable from the remote base
        # directory.
        self.delete(key=None)
        # Delete the remote base directory itself.
        self.client.sftp().rmdir(self.remotedir)

    def load(self, key: str) -> IOHandle:
        """Load a file object at the source path of this volume store.

        Returns a file handle that can be used to open and read the file.

        Parameters
        ----------
        key: str
            Path to a file object in the storage volume.

        Returns
        --------
        flowserv.volume.base.IOHandle
        """
        # The file key is a path expression that uses '/' as the path separator.
        # If the local OS uses a different separator we need to replace it.
        filename = util.filepath(key=key, sep=self.client.sep)
        filename = self.client.sep.join([self.remotedir, filename])
        return SFTPFile(filename=filename, client=self.client)

    def store(self, file: IOHandle, dst: str):
        """Store a given file object at the destination path of this volume
        store.

        Parameters
        ----------
        file: flowserv.volume.base.IOHandle
            File-like object that is being stored.
        dst: str
            Destination path for the stored object.
        """
        # The file key is a path expression that uses '/' as the path separator.
        # If the local OS uses a different separator we need to replace it.
        filename = util.filepath(key=dst, sep=self.client.sep)
        filename = self.client.sep.join([self.remotedir, filename])
        dirname = self.client.sep.join(filename.split(self.client.sep)[:-1])
        sftp = self.client.sftp()
        try:
            sftp_mkdir(client=sftp, dirpath=dirname)
            with sftp.open(filename, 'wb') as fout:
                with file.open() as fin:
                    fout.write(fin.read())
        finally:
            sftp.close()

    def walk(self, src: str) -> List[Tuple[str, IOHandle]]:
        """Get list of all files at the given source path.

        If the source path references a single file the returned list will
        contain a single entry. If the source specifies a folder the result
        contains a list of all files in that folder and the subfolders.

        Parameters
        ----------
        src: str
            Source path specifying a file or folder.

        Returns
        -------
        list of tuples (str, flowserv.volume.base.IOHandle)
        """
        dirpath = util.filepath(key=src, sep=self.client.sep)
        dirpath = self.client.sep.join([self.remotedir, dirpath]) if dirpath else self.remotedir
        files = self.client.walk(dirpath=dirpath)
        if files is None:
            # The source path references a single file.
            filename = util.filepath(key=src, sep=self.client.sep)
            filename = self.client.sep.join([self.remotedir, filename])
            return [(src, SFTPFile(filename=filename, client=self.client))]
        else:
            # The source path references a directory.
            result = list()
            for key in files:
                filename = util.filepath(key=key, sep=self.client.sep)
                filename = self.client.sep.join([self.remotedir, filename])
                result.append((key, SFTPFile(filename=filename, client=self.client)))
            return result


# -- Helper functions ---------------------------------------------------------

def sftp_mkdir(client: paramiko.SFTPClient, dirpath: str):
    """Create a directory on the remote server.

    ----------
    client: paramiko.SFTPClient
        SFTP client.
    dirpath: string
        Path to the created directory on the remote server.
    """
    try:
        # Attempt to change into the directory. This will raise an error
        # if the directory does not exist.
        client.chdir(dirpath)
    except IOError:
        # Create directory if it does not exist.
        client.mkdir(dirpath)
