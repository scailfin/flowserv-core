# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Fixtures fro unit tests."""

import os
import pytest
import shutil

import flowserv.util.ssh as ssh


# -- Patch SSH Client ---------------------------------------------------------

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
        self._active = 2

    @property
    def active(self):
        return self._active > 0

    def chdir(self, dirpath):
        if not os.path.isdir(dirpath):
            raise IOError('does not exist')

    def close(self):
        pass

    def exec_command(self, command):
        output = Channel(command, exit_status=self.exit_status)
        return None, output, output

    def get(self, src, dst):
        return self.put(src, dst)

    def get_transport(self):
        self._active -= 1
        return self

    def listdir_attr(self, filename):
        return [FH(f) for f in os.listdir(filename)]

    def mkdir(self, dirpath):
        os.makedirs(dirpath, exist_ok=False)

    def open(self, filename, mode):
        return open(filename, mode + 'b')

    def open_sftp(self):
        return self

    def put(self, src, dst, confirm=None):
        shutil.copy(src=src, dst=dst)

    def remove(self, filename):
        os.remove(filename)

    def rmdir(self, dirpath):
        os.rmdir(dirpath)


@pytest.fixture
def mock_ssh(monkeypatch):
    """Path for generating the SSH client."""
    def mock_paramiko_client(*args, **kwargs):
        return SSHTestClient()

    monkeypatch.setattr(ssh, "paramiko_ssh_client", mock_paramiko_client)
