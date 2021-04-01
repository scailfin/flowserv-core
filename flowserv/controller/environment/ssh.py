# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Workflow environment manager that uses a SSH client to connect to a remote
server where run files are maintained.
"""

from pathlib import Path
from typing import Optional, Tuple

import os
import paramiko

from flowserv.controller.environment.base import RunEnvironment
from flowserv.model.files.base import IOHandle
from flowserv.util.ssh import SSHClient, walk


class SSHEnvironment(RunEnvironment):
    """Runtime file manager that connects to a remote server via sftp."""
    def __init__(self, client: SSHClient, remotedir: str, identifier: Optional[str] = None):
        """Initialize the run base directory on the remote server and the SSH
        connection parameters.

        The remote base directory is created if it does not exist. If no
        identifier is provided a unique identifier is generated by the super
        class constructor.

        Parameters
        ----------
        client: flowserv.util.ssh.SSHClient
            SSH client for accessing the remote server.
        remotedir: string
            Base directory for all run files on the remote file system.
        identifier: string
            Unique identifier.
        """
        super(SSHEnvironment, self).__init__(identifier=identifier)
        self.client = client
        self.remotedir = remotedir
        # Create the remote directory if it does not exists.
        sftp_mkdir(client=client.sftp(), dirpath=self.remotedir)

    def download(self, src: str, dst: str):
        """Download the file or folder at the source path to the given
        destination.

        The source path is relative to the base directory for the workflow run.
        The destination path is absolute or relative to the current working
        directory.

        Parameters
        ----------
        src: string
            Relative source path on the environment run directory.
        dst: string
            Absolute or relative path on the local file system.
        """
        # Get the sftp client.
        sftp = self.client.sftp()
        # Ensure that the parent folder for the destination file exists.
        os.makedirs(Path(dst).parent.absolute(), exist_ok=True)
        # Recursively walk through the remote folder to get the list of all
        # files that need to be copied. If the src points to a file the result
        # of the walk will be empty.
        source = os.path.join(self.remotedir, src)
        files = walk(client=sftp, dirpath=source)
        if files is not None:
            # Create all target directories before downloading files.
            for dirpath in {dp for dp, _ in files if dp}:
                os.makedirs(os.path.join(dst, dirpath), exist_ok=True)
            # Download files.
            for dirpath, filename in files:
                if dirpath:
                    srcpath = os.path.join(source, dirpath, filename)
                    dstpath = os.path.join(dst, dirpath, filename)
                else:
                    srcpath = os.path.join(source, filename)
                    dstpath = os.path.join(dst, filename)
                sftp.get(srcpath, dstpath)
        else:
            sftp.get(source, dst)

    def erase(self):
        """Erase the run environment base directory and all its contents."""
        # Collect sub-directories that need to be removed separately after
        # the directories are empty.
        directories = set()
        sftp = self.client.sftp()
        # Get recursive list of all files in the base folder and delete them.
        for dirname, filename in self.client.walk(self.remotedir):
            if dirname is not None:
                directories.add(dirname)
            f = os.path.join(dirname, filename) if dirname is not None else filename
            sftp.remove(os.path.join(self.remotedir, f))
        for dirpath in sorted(directories, reverse=True):
            sftp.rmdir(os.path.join(self.remotedir, dirpath))
        # Delete the remote base directory itself.
        sftp.rmdir(self.remotedir)
        # Close the client.
        self.client.close()

    def upload(self, src: Tuple[IOHandle, str], dst: str):
        """Upload a file or folder to the run environment.

        The destination is relative to the base directory of the run
        environment.

        Parameters
        ----------
        src: string or flowserv.model.files.base.IOHandle
            Source file or folder that is being uploaded to the run environment.
        dst: string
            Relative target path for the uploaded files.
        """
        # Get the sftp client.
        sftp = self.client.sftp()
        # Ensure that all directories exist on the remote server.
        directories = list()
        parent, _ = os.path.split(dst)
        while parent:
            directories.append(parent)
            parent, _ = os.path.split(parent)
        while directories:
            dirpath = directories.pop()
            sftp_mkdir(client=sftp, dirpath=os.path.join(self.remotedir, dirpath))
        # Create the absolute target path.
        target = os.path.join(self.remotedir, dst)
        if isinstance(src, IOHandle):
            with sftp.open(target, 'w') as f:
                f.write(src.open().read())
        elif os.path.isdir(src):
            copy_folder(client=sftp, src=src, dst=target)
        else:  # assume that src points to a file on the local file system.
            sftp.put(src, target)


# -- Helper functions ---------------------------------------------------------

def copy_folder(client: paramiko.SFTPClient, src: str, dst: str):
    """Copy all files in a source folder to a target folder on the remote
    server.

    Parameters
    ----------
    client: paramiko.SFTPClient
        SFTP client.
    src: string
        Path to the source folder.
    dst: string
        Path on the file system to the target folder.
    """
    sftp_mkdir(client=client, dirpath=dst)
    # Copy all files and folders from the source folder to the target
    # folder.
    for filename in os.listdir(src):
        source = os.path.join(src, filename)
        target = os.path.join(dst, filename)
        if os.path.isdir(source):
            copy_folder(client=client, src=source, dst=target)
        else:
            client.put(source, target)


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
