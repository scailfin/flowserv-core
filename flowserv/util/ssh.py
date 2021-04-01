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
from typing import List, Optional, Tuple

import paramiko


class SSHClient:
    """SSH client that allows to run remote commands and access files."""
    def __init__(
        self, hostname: str, port: Optional[int] = None, timeout: Optional[float] = None,
        look_for_keys: Optional[bool] = False
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
        """
        self._client = paramiko_ssh_client(
            hostname=hostname,
            port=port,
            timeout=timeout,
            look_for_keys=look_for_keys
        )

    def close(self):
        """Close the SSH client."""
        self._client.close()

    def download(self, src: str, dst: str):
        """Download the specified source file to a given target path.

        Parameters
        ----------
        src: string
            Path to source file on the remote server.
        dst: string
            Destination path for the downloaded file.
        """
        # Get a new SFTP client.
        sftp = self._client.open_sftp()
        try:
            sftp.get(src, dst)
        finally:
            sftp.close()

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
        _, stdout, stderr = self._client.exec_command(command)
        if stdout.channel.recv_exit_status() != 0:
            raise RuntimeError(stderr.read().decode("utf-8"))
        return stdout.read().decode("utf-8")

    def upload(self, files: List[Tuple[str, str]], directories: List[str]):
        """Upload a given list of files.

        Files are given as pairs of source and target path. The list of unique
        target directories contains the list of all directories that need to
        exist on the remote server for the uploaded files. These directories
        are created in advance before attempting to upload the files.

        Parameters
        ----------
        files: list of tuples of (string, string)
            Source and target path for uploaded files. All path expressions
            should be absolute paths expressions.
        directories: list of string
            List of target directories that need to exist on the remove server.
            These directories will be created if they don't exist.
        """
        # Get a new SFTP client.
        sftp = self._client.open_sftp()
        try:
            # Attempt to create all required target directories on the remote
            # server first.
            for dirpath in directories:
                try:
                    sftp.mkdir(dirpath)
                except Exception:
                    pass
            # Upload the files.
            for src, dst in files:
                sftp.put(src, dst, confirm=True)
        finally:
            sftp.close()


# -- Helper Method ------------------------------------------------------------

def paramiko_ssh_client(
    hostname: str, port: Optional[int] = None, timeout: Optional[float] = None,
    look_for_keys: Optional[bool] = False
) -> paramiko.SSHClient:  # pragma: no cover
    """Helper function to create a paramiko SSH Client.

    This separate function is primarily intended to make pathcing easier for
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
    look_for_keys: Optional[bool] = False
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

    Returns
    -------
    paramiko.SSHClient
    """
    client = SSHClient(
        hostname=hostname,
        port=port,
        timeout=timeout,
        look_for_keys=look_for_keys
    )
    try:
        yield client
    finally:
        client.close()
