# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the SSH client."""

from pathlib import Path

import os
import pytest

import flowserv.util.ssh as ssh


# -- Patch for SSHClient ------------------------------------------------------

class Channel:
    def __init__(self, command, exit_status=0):
        self.command = command
        self.exit_status = exit_status

    @property
    def channel(self):
        return self

    def read(self):
        return self.command.encode('utf-8')

    def recv_exit_status(self):
        return self.exit_status


class FH:
    """File handle that provides the filename property."""
    def __init__(self, filename):
        self.filename = filename


class SSHTestClient:
    """Fake SSH client for test purposes."""
    def __init__(self):
        self.exit_status = 0
        self._dirs = set()

    def close(self):
        pass

    def exec_command(self, command):
        output = Channel(command, exit_status=self.exit_status)
        return None, output, output

    def get(self, src, dst):
        pass

    def listdir_attr(self, filename):
        return [FH(f) for f in os.listdir(filename)]

    def mkdir(self, dirpath):
        if dirpath in self._dirs:
            raise RuntimeError('exists')
        self._dirs.add(dirpath)

    def open_sftp(self):
        return self

    def put(self, src, dst, confirm):
        pass


@pytest.fixture
def mock_ssh(monkeypatch):
    """Path for generating the SSH client."""
    def mock_paramiko_client(*args, **kwargs):
        return SSHTestClient()

    monkeypatch.setattr(ssh, "paramiko_ssh_client", mock_paramiko_client)


# -- Unit tests ---------------------------------------------------------------

def test_ssh_execute_command(mock_ssh):
    """Test executing commands using the SSH client."""
    with ssh.ssh_client('test') as client:
        assert client.exec_cmd('ls') == 'ls'
    # -- Simulate error case --------------------------------------------------
    with ssh.ssh_client('test') as client:
        client._client.exit_status = -1
        with pytest.raises(RuntimeError):
            client.exec_cmd('ls')


def test_ssh_upload_download(mock_ssh):
    """Test file uploads and downloads via the SSH client."""
    with ssh.ssh_client('test') as client:
        client.upload(files=[], directories=[])
        client.upload(files=[('a', 'b')], directories=['a', 'a'])
        client.download('b', 'c')


def test_ssh_walk(mock_ssh, tmpdir):
    """Test the remote directory walk method."""
    # -- Setup ----------------------------------------------------------------
    #
    # Create directory structure:
    # a.txt
    # b/
    # b/c.txt
    # b/d.txt
    # b/e/
    # b/e/f.txt
    # b/g
    os.makedirs(os.path.join(tmpdir, 'b', 'e'))
    os.makedirs(os.path.join(tmpdir, 'b', 'g'))
    Path(os.path.join(tmpdir, 'a.txt')).touch()
    Path(os.path.join(tmpdir, 'b', 'c.txt')).touch()
    Path(os.path.join(tmpdir, 'b', 'd.txt')).touch()
    Path(os.path.join(tmpdir, 'b', 'e', 'f.txt')).touch()
    Path(os.path.join(tmpdir, 'a.txt')).touch()
    # -- Test -----------------------------------------------------------------
    with ssh.ssh_client('test') as client:
        files = client.walk(tmpdir)
    assert len(files) == 4
    assert (None, 'a.txt') in files
    assert ('b', 'c.txt') in files
    assert ('b', 'd.txt') in files
    assert ('b/e', 'f.txt') in files
