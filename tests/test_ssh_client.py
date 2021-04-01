# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the SSH client."""

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
