# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""SSH client interface for interacting with remote servers using the paramiko
package.
"""

from contextlib import contextmanager
from typing import List, Optional

import paramiko
import flowserv.util.files as util


class SSHClient:
    """SSH client that allows to run remote commands and access files."""
    def __init__(
        self, hostname: str, port: Optional[int] = None, timeout: Optional[float] = None,
        look_for_keys: Optional[bool] = False, sep: Optional[str] = '/'
    ):
        """Create SSH client.

        Parameters
        ----------
        hostname: string
            Server to connect to.
        port: int, default=None
            Server port to connect to.
        timeout: float, default=None
            Optional timeout (in seconds) for the TCP connect.
        look_for_keys: bool, default=False
            Set to True to enable searching for discoverable private key files
            in ``~/.ssh/``.
        sep: string, default='/'
            Path separator used by the remote file system.
        """
        self.hostname = hostname
        self.port = port
        self.timeout = timeout
        self.look_for_keys = look_for_keys
        self.sep = sep
        # Create the SSH client.
        self._client = None
        self.ssh_client

    def close(self):
        """Close the SSH client."""
        self._client.close()

    def exec_cmd(self, command) -> str:
        """Execute command on the remote server.

        Returns output from STDOUT. Raises an error if command execution on the
        remote server failed (as indicated by the program exit code).

        Parameters
        ----------
        command: string
            Command line string that is executed on the remote server.

        Returns
        -------
        string
        """
        _, stdout, stderr = self.ssh_client.exec_command(command)
        if stdout.channel.recv_exit_status() != 0:
            raise RuntimeError(stderr.read().decode("utf-8"))
        return stdout.read().decode("utf-8")

    def sftp(self) -> paramiko.SFTPClient:
        """Get SFTP client.

        Returns
        -------
        paramiko.SFTPClient
        """
        return self.ssh_client.open_sftp()

    @property
    def ssh_client(self) -> paramiko.SSHClient:
        """Get an active instance of the SSH Client.

        Returns
        -------
        paramiko.SSHClient
        """
        if not self._client or not self._client.get_transport().active:
            self._client = paramiko_ssh_client(
                hostname=self.hostname,
                port=self.port,
                timeout=self.timeout,
                look_for_keys=self.look_for_keys
            )
        return self._client

    def walk(self, dirpath: str) -> List[str]:
        """Get recursive listing of all files in a given directory.

        Returns a list of relative path expressions for files in the directory.

        If ``dirpath`` does not reference a directory the result is None.

        Parameters
        ----------
        dirpath: string
            Path to a directory on the remote server.

        Returns
        -------
        list of string
        """
        # Get a new SFTP client.
        sftp = self.sftp()
        try:
            # Recursively walk the directory path.
            return walk(client=sftp, dirpath=dirpath, sep=self.sep)
        finally:
            sftp.close()


# -- Helper Method ------------------------------------------------------------

def paramiko_ssh_client(
    hostname: str, port: Optional[int] = None, timeout: Optional[float] = None,
    look_for_keys: Optional[bool] = False
) -> paramiko.SSHClient:  # pragma: no cover
    """Helper function to create a paramiko SSH Client.

    This separate function is primarily intended to make patching easier for
    unit testing.

    Parameters
    ----------
    hostname: string
        Server to connect to.
    port: int, default=None
        Server port to connect to.
    timeout: float, default=None
        Optional timeout (in seconds) for the TCP connect.
    look_for_keys: bool, default=False
        Set to True to enable searching for discoverable private key files
        in ``~/.ssh/``.

    Returns
    -------
    paramiko.SSHClient
    """
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        hostname=hostname,
        port=port,
        timeout=timeout,
        look_for_keys=look_for_keys
    )
    return client


@contextmanager
def ssh_client(
    hostname: str, port: Optional[int] = None, timeout: Optional[float] = None,
    look_for_keys: Optional[bool] = False, sep: Optional[str] = '/'
) -> SSHClient:
    """Context manager for the flowserv SSHCilent.

    Parameters
    ----------
    hostname: string
        Server to connect to.
    port: int, default=None
        Server port to connect to.
    timeout: float, default=None
        Optional timeout (in seconds) for the TCP connect.
    look_for_keys: bool, default=False
        Set to True to enable searching for discoverable private key files
        in ``~/.ssh/``.
    sep: string, default='/'
        Path separator used by the remote file system.

    Returns
    -------
    paramiko.SSHClient
    """
    client = SSHClient(
        hostname=hostname,
        port=port,
        timeout=timeout,
        look_for_keys=look_for_keys,
        sep=sep
    )
    try:
        yield client
    finally:
        client.close()


def walk(
    client: paramiko.SFTPClient, dirpath: str, prefix: Optional[str] = None,
    sep: Optional[str] = '/'
) -> List[str]:
    """Recursively scan contents of a remote directory.

    Returns a list of tuples that contain the relative sub-directory path
    and the file name for all files. The sub-directory path for files in
    the ``dirpath`` is None.

    If ``dirpath`` does not reference a directory the result is None.

    Parameters
    ----------
    client: paramiko.SFTPClient
        SFTP client.
    dirpath: string
        Path to a directory on the remote server.
    prefix: string, default=None
        Prefix path for the current (sub-)directory.
    sep: string, default='/'
        Path separator used by the remote file system.

    Returns
    -------
    list of tuples of (string, string)
    """
    result = list()
    try:
        for f in client.listdir_attr(dirpath):
            children = walk(
                client=client,
                dirpath=sep.join([dirpath, f.filename]),
                prefix=util.join(prefix, f.filename) if prefix else f.filename
            )
            if children is not None:
                # The file is a directory.
                result.extend(children)
            else:
                # Couldn't recursively explore the filename, i.e., it is not a
                # directory but a file.
                result.append(util.join(prefix, f.filename) if prefix else f.filename)
    except IOError:
        # An error is raised if the dirpath does not reference a valid
        # directory.
        return None
    return result
